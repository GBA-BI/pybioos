#!/usr/bin/env python3
# coding: utf-8

import argparse
import logging
import sys

from bioos import bioos
from bioos.config import Config
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

    args = parser.parse_args()
    logger = get_logger()

    try:
        # 配置Bio-OS
        Config.set_access_key(args.ak)
        Config.set_secret_key(args.sk)
        Config.set_endpoint("https://bio-top.miracle.ac.cn")

        # 获取workspace ID
        workspaces = bioos.list_workspaces()
        workspace_info = workspaces.query(f"Name=='{args.workspace_name}'")
        if workspace_info.empty:
            logger.error(f"Workspace {args.workspace_name} not found")
            sys.exit(1)
        workspace_id = workspace_info["ID"].iloc[0]

        # 获取提交的运行状态
        resp = Config.service().list_runs({
            "SubmissionID": args.submission_id,
            "WorkspaceID": workspace_id
        })

        if not resp.get("Items"):
            logger.error(f"No runs found for submission {args.submission_id}")
            sys.exit(1)

        # 打印所有运行的状态
        print(f"\nSubmission ID: {args.submission_id}")
        print("Runs Status:")
        print("-" * 60)
        print(f"{'Run ID':<40} {'Status':<10} {'Message'}")
        print("-" * 60)

        for run in resp.get("Items"):
            run_id = run.get("ID", "N/A")
            status = run.get("Status", "Unknown")
            message = run.get("Message", "")
            print(f"{run_id:<40} {status:<10} {message}")

    except Exception as e:
        logger.error(f"Error: {str(e)}")
        sys.exit(1)


if __name__ == '__main__':
    bioos_workflow_status_check()
