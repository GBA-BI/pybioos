#!/usr/bin/env python3
# coding: utf-8

import argparse
import logging
import os
import sys

from bioos import bioos
from bioos.config import Config


def get_logger():
    """Setup logger"""
    logger = logging.getLogger('get_submission_logs')
    if not logger.handlers:
        handler = logging.StreamHandler()
        formatter = logging.Formatter(
            '%(asctime)s - %(levelname)s - %(message)s')
        handler.setFormatter(formatter)
        logger.addHandler(handler)
        logger.setLevel(logging.INFO)
    return logger


def get_submission_logs():
    """Command line entry point for downloading workflow submission logs"""
    parser = argparse.ArgumentParser(
        description='Bio-OS Workflow Submission Logs Download Tool',
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
                        help='ID of the submission to download logs')
    parser.add_argument(
        '--output_dir',
        default='.',
        help='Local directory to save the logs (default: current directory)')

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

        # 获取workspace对象
        ws = bioos.workspace(workspace_id)

        # 列出所有文件
        logger.info(f"Listing files for submission {args.submission_id}")
        files_df = ws.files.list()

        # 过滤出与submission相关的日志文件
        log_files = []
        for file in files_df.key:
            if args.submission_id in file:
                # 检查是否是日志文件
                if (file.endswith('.log') or 'stderr' in file
                        or 'stdout' in file or '/log/' in file):
                    log_files.append(file)

        if not log_files:
            logger.error(
                f"No log files found for submission {args.submission_id}")
            sys.exit(1)

        logger.info(f"Found {len(log_files)} log files")

        # 创建输出目录
        output_path = os.path.join(args.output_dir, args.submission_id)
        os.makedirs(output_path, exist_ok=True)

        # 下载文件
        logger.info("Downloading log files...")
        try:
            ws.files.download(log_files, output_path, flatten=False)
            logger.info(f"Successfully downloaded log files to {output_path}")
        except Exception as e:
            logger.error(f"Error downloading some files: {str(e)}")
            logger.info("Continuing with successfully downloaded files...")

        # 打印下载的文件列表
        logger.info("\nDownloaded files:")
        for root, _, files in os.walk(output_path):
            for file in files:
                rel_path = os.path.relpath(os.path.join(root, file),
                                           output_path)
                logger.info(f"  {rel_path}")

    except Exception as e:
        logger.error(f"Error: {str(e)}")
        sys.exit(1)


if __name__ == '__main__':
    get_submission_logs()
