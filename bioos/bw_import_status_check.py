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
    logger = logging.getLogger('bw_import_status_check')
    if not logger.handlers:
        handler = logging.StreamHandler()
        formatter = logging.Formatter(
            '%(asctime)s - %(levelname)s - %(message)s')
        handler.setFormatter(formatter)
        logger.addHandler(handler)
        logger.setLevel(logging.INFO)
    return logger


def bioos_workflow_status_check():
    """Command line entry point for checking workflow validation status"""
    parser = argparse.ArgumentParser(
        description='Bio-OS Workflow Import Status Check Tool',
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
    parser.add_argument('--workflow_id',
                        required=True,
                        help='ID of the workflow to check')

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

        # 创建WorkflowResource实例
        workflow_resource = WorkflowResource(workspace_id)

        # 获取工作流状态
        df = workflow_resource.list()
        workflow_info = df[df.ID == args.workflow_id]

        if len(workflow_info) == 1:
            status = workflow_info.iloc[0]["Status"]["Phase"]
            message = workflow_info.iloc[0]["Status"].get("Message", "")

            # 打印状态信息
            print(f"Status: {status}")
            if message:
                print(f"Message: {message}")

        else:
            logger.error(f"Workflow with ID {args.workflow_id} not found")
            sys.exit(1)

    except Exception as e:
        logger.error(f"Error: {str(e)}")
        sys.exit(1)


if __name__ == '__main__':
    bioos_workflow_status_check()
