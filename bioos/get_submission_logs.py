import argparse
import logging
import os
import sys

from bioos import bioos
from bioos.config import DEFAULT_ENDPOINT
from bioos.ops.auth import login_to_bioos, resolve_workspace


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


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description='Bio-OS Workflow Submission Logs Download Tool',
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument('--ak', required=False, help='Access key for your Bio-OS instance platform account')
    parser.add_argument('--sk', required=False, help='Secret key for your Bio-OS instance platform account')
    parser.add_argument('--workspace_name', required=True, help='Target workspace name')
    parser.add_argument('--submission_id', required=True, help='ID of the submission to download logs')
    parser.add_argument('--output_dir', default='.', help='Local directory to save the logs (default: current directory)')
    parser.add_argument('--endpoint', help='Bio-OS instance platform endpoint', default=DEFAULT_ENDPOINT)
    return parser


def handle(args) -> str:
    logger = get_logger()
    login_to_bioos(access_key=args.ak, secret_key=args.sk, endpoint=args.endpoint)
    workspace_id, _ = resolve_workspace(args.workspace_name)
    ws = bioos.workspace(workspace_id)

    logger.info(f"Listing files for submission {args.submission_id}")
    files_df = ws.files.list(recursive=True)

    log_files = []
    for file in files_df.key:
        if args.submission_id in file:
            if (
                file.endswith('.log')
                or 'stderr' in file
                or 'stdout' in file
                or '/log/' in file
                or file.endswith('/log')
                or file.endswith('/rc')
                or file.endswith('/script')
            ):
                log_files.append(file)

    if not log_files:
        raise RuntimeError(f"No log files found for submission {args.submission_id}")

    logger.info(f"Found {len(log_files)} log files")
    output_path = os.path.join(args.output_dir, args.submission_id)
    os.makedirs(output_path, exist_ok=True)

    logger.info("Downloading log files...")
    try:
        ws.files.download(log_files, output_path, flatten=False)
        logger.info(f"Successfully downloaded log files to {output_path}")
    except Exception as exc:
        logger.error(f"Error downloading some files: {str(exc)}")
        logger.info("Continuing with successfully downloaded files...")

    downloaded_files = []
    for root, _, files in os.walk(output_path):
        for file in files:
            rel_path = os.path.relpath(os.path.join(root, file), output_path)
            downloaded_files.append(rel_path)

    lines = [f"Downloaded files to {output_path}:"]
    lines.extend(f"  {item}" for item in downloaded_files)
    return "\n".join(lines)


def get_submission_logs():
    """Command line entry point for downloading workflow submission logs"""
    parser = build_parser()
    args = parser.parse_args()
    logger = get_logger()
    try:
        print(handle(args))
        sys.exit(0)
    except Exception as exc:
        logger.error(f"Error: {exc}")
        sys.exit(1)


if __name__ == '__main__':
    get_submission_logs()
