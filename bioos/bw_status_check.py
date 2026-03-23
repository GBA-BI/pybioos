import argparse
import json
import logging
import sys

from bioos.config import Config, DEFAULT_ENDPOINT
from bioos.ops.auth import login_to_bioos, resolve_workspace


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


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description='Bio-OS Workflow Run Status Check Tool',
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument('--ak', required=False, help='Access key for your Bio-OS instance platform account')
    parser.add_argument('--sk', required=False, help='Secret key for your Bio-OS instance platform account')
    parser.add_argument('--workspace_name', required=True, help='Target workspace name')
    parser.add_argument('--submission_id', required=True, help='ID of the submission to check')
    parser.add_argument('--endpoint', help='Bio-OS instance platform endpoint', default=DEFAULT_ENDPOINT)
    parser.add_argument('--page_size', type=int, default=0, help='Page size for listing runs (0 for all, default: 0)')
    return parser


def handle(args) -> str:
    login_to_bioos(access_key=args.ak, secret_key=args.sk, endpoint=args.endpoint)
    workspace_id, _ = resolve_workspace(args.workspace_name)
    params = {
        "SubmissionID": args.submission_id,
        "WorkspaceID": workspace_id,
        "PageSize": args.page_size,
    }
    resp = Config.service().list_runs(params)

    if not resp.get("Items"):
        raise RuntimeError(f"No runs found for submission {args.submission_id}")

    lines = [
        f"Submission ID: {args.submission_id}",
        "Runs Status:",
        "-" * 240,
        f"{'Run ID':<40} {'Status':<12} {'Message':<120} {'Outputs'}",
        "-" * 240,
    ]

    for run in resp.get("Items"):
        run_id = run.get("ID", "N/A")
        status = run.get("Status", "Unknown")
        message = run.get("Message", "") or ""
        outputs = run.get("Outputs", "") or ""

        if status == "Failed" and message:
            output_str = ""
        elif status == "Succeeded" and outputs:
            try:
                outputs_dict = json.loads(outputs)
                all_files = []
                for _, value in outputs_dict.items():
                    if isinstance(value, list):
                        all_files.extend(value)
                    elif isinstance(value, str):
                        all_files.append(value)
                output_str = ", ".join(all_files) if all_files else outputs
            except Exception:
                output_str = outputs
            message = "Succeeded"
        else:
            output_str = ""

        lines.append(f"{run_id:<40} {status:<12} {message:<60} {output_str}")

    lines.append("-" * 180)
    return "\n".join(lines)


def bioos_workflow_status_check():
    """Command line entry point for checking workflow run status"""
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
    bioos_workflow_status_check()
