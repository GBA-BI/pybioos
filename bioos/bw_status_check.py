#!/usr/bin/env python3
# coding: utf-8

import argparse
import json
import logging
import sys

from bioos import bioos
from bioos.config import Config, DEFAULT_ENDPOINT
from bioos.resource.workflows import WorkflowResource


def get_logger():
    """Setup logger"""
    logger = logging.getLogger('bw_status_check')
    if not logger.handlers:
        handler = logging.StreamHandler()
        formatter = logging.Formatter(
            '%(asctime)s - %(levelname)s - %(message)s')
        handler.setFormatter(formatter)
        logger.addHandler(handler)
        logger.setLevel(logging.INFO)
    return logger


def bioos_workflow_status_check():
    """Command line entry point for checking workflow run status"""
    parser = argparse.ArgumentParser(
        description='Bio-OS Workflow Run Status Check Tool',
        formatter_class=argparse.RawDescriptionHelpFormatter)

    # 必需参数
    parser.add_argument(
        '--ak',
        required=True,
        help='Access key for your Bio-OS instance platform account')
    parser.add_argument(
        '--sk',
        required=True,
        help='Secret key for your Bio-OS instance platform account')
    parser.add_argument('--workspace_name',
                        required=True,
                        help='Target workspace name')
    parser.add_argument('--submission_id',
                        required=True,
                        help='ID of the submission to check')
    
    # 可选参数
    parser.add_argument('--endpoint',
                        help='Bio-OS instance platform endpoint',
                        default=DEFAULT_ENDPOINT)
    parser.add_argument('--page_size',
                        type=int,
                        default=0,
                        help='Page size for listing runs (0 for all, default: 0)')

    args = parser.parse_args()
    logger = get_logger()

    try:
        # 配置Bio-OS
        Config.set_access_key(args.ak)
        Config.set_secret_key(args.sk)
        Config.set_endpoint(args.endpoint)

        # 获取workspace ID
        workspaces = bioos.list_workspaces()
        workspace_info = workspaces.query(f"Name=='{args.workspace_name}'")
        if workspace_info.empty:
            logger.error(f"Workspace {args.workspace_name} not found")
            sys.exit(1)
        workspace_id = workspace_info["ID"].iloc[0]

        # 获取提交的运行状态
        params = {
            "SubmissionID": args.submission_id,
            "WorkspaceID": workspace_id,
            "PageSize": args.page_size
        }
        resp = Config.service().list_runs(params)

        if not resp.get("Items"):
            logger.error(f"No runs found for submission {args.submission_id}")
            sys.exit(1)

        # 打印所有运行的状态
        print(f"\nSubmission ID: {args.submission_id}")
        print("Runs Status:")
        print("-" * 240)
        print(f"{'Run ID':<40} {'Status':<12} {'Message':<120} {'Outputs'}")
        print("-" * 240)

        for run in resp.get("Items"):
            run_id = run.get("ID", "N/A")
            status = run.get("Status", "Unknown")
            message = run.get("Message", "") or ""
            outputs = run.get("Outputs", "") or ""
            
            # 失败时显示错误信息，成功时显示输出文件
            if status == "Failed" and message:
                # 失败时显示完整错误信息
                output_str = ""
            elif status == "Succeeded" and outputs:
                # 解析 Outputs JSON 并提取所有文件路径，全量返回
                try:
                    outputs_dict = json.loads(outputs)
                    # 提取所有输出文件路径
                    all_files = []
                    for key, value in outputs_dict.items():
                        if isinstance(value, list):
                            all_files.extend(value)
                        elif isinstance(value, str):
                            all_files.append(value)
                    # 全量显示所有文件路径
                    if all_files:
                        output_str = ", ".join(all_files)
                    else:
                        output_str = outputs
                except:
                    output_str = outputs
                message = "Succeeded"
            else:
                output_str = ""
                # 其他状态也显示完整信息
            
            print(f"{run_id:<40} {status:<12} {message:<60} {output_str}")
        
        print("-" * 180)

    except Exception as e:
        logger.error(f"Error: {str(e)}")
        sys.exit(1)


if __name__ == '__main__':
    bioos_workflow_status_check()
