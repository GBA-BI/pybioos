#!/usr/bin/env python3
# coding: utf-8

import argparse
import logging
import os
import sys
import time

from bioos import bioos
from bioos.config import Config
from bioos.resource.workflows import WorkflowResource


def get_logger():
    """Setup logger"""
    logger = logging.getLogger('bw_import')
    if not logger.handlers:
        handler = logging.StreamHandler()
        formatter = logging.Formatter(
            '%(asctime)s - %(levelname)s - %(message)s')
        handler.setFormatter(formatter)
        logger.addHandler(handler)
        logger.setLevel(logging.INFO)
    return logger


def bioos_workflow_import():
    """Command line entry point"""
    parser = argparse.ArgumentParser(
        description='Bio-OS Workflow Import Tool',
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
    parser.add_argument('--workflow_name',
                        required=True,
                        help='Name for the workflow to be imported')
    parser.add_argument('--workflow_source',
                        required=True,
                        help='Local WDL file path or git repository URL')

    # 可选参数
    parser.add_argument('--workflow_desc',
                        help='Description for the workflow',
                        default='')
    parser.add_argument(
        '--main_path',
        help='Main workflow file path (required for git repository)',
        default='')
    parser.add_argument(
        '--monitor',
        action='store_true',
        help='Monitor the workflow validation status until completion')
    parser.add_argument(
        '--monitor_interval',
        type=int,
        default=60,
        help='Time interval in seconds for checking workflow status')

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

        # 导入workflow
        try:
            result = workflow_resource.import_workflow(
                source=args.workflow_source,
                name=args.workflow_name,
                description=args.workflow_desc,
                language="WDL",
                main_workflow_path=args.main_path)
            logger.info(
                f"Successfully uploaded workflow: {result}, validating..., please wait..."
            )

            # 如果设置了monitor参数，则监控工作流状态
            if args.monitor:
                max_retries = 10  # 最大重试次数
                retry_count = 0

                while retry_count < max_retries:
                    df = workflow_resource.list()
                    workflow_info = df[df.Name == args.workflow_name]

                    if len(workflow_info) == 1:
                        status = workflow_info.iloc[0]["Status"]["Phase"]

                        if status == "Succeeded":
                            logger.info(
                                f"Workflow {args.workflow_name} validated successfully"
                            )
                            sys.exit(0)
                        elif status == "Failed":
                            logger.error(
                                f"Workflow {args.workflow_name} validation failed"
                            )
                            sys.exit(1)
                        elif status == "Importing":
                            logger.info(
                                f"Workflow {args.workflow_name} is still validating, please wait..."
                            )
                            time.sleep(args.monitor_interval)
                            retry_count += 1
                        else:
                            logger.error(
                                f"Workflow {args.workflow_name} has unknown status: {status}"
                            )
                            sys.exit(1)
                    else:
                        logger.error(
                            f"Workflow {args.workflow_name} not found after import"
                        )
                        sys.exit(1)

                logger.error(
                    f"Workflow validation timeout after {max_retries} retries")
                sys.exit(1)
            else:
                # 如果没有设置monitor参数，直接退出
                logger.info(
                    f"Workflow {args.workflow_name} is still validating, {result}, please wait and check the status later."
                )
                sys.exit(0)

        except Exception as e:
            logger.error(f"Failed to import workflow: {str(e)}")
            sys.exit(1)

    except Exception as e:
        logger.error(f"Error: {str(e)}")
        sys.exit(1)


if __name__ == '__main__':
    bioos_workflow_import()
