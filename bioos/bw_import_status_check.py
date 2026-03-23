import argparse
import logging
import sys

from bioos.config import DEFAULT_ENDPOINT
from bioos.ops.auth import login_to_bioos, resolve_workspace
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


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description='Bio-OS Workflow Import Status Check Tool',
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument('--ak', required=False, help='Access key for your Bio-OS instance platform account')
    parser.add_argument('--sk', required=False, help='Secret key for your Bio-OS instance platform account')
    parser.add_argument('--workspace_name', required=True, help='Target workspace name')
    parser.add_argument('--workflow_id', required=True, help='ID of the workflow to check')
    parser.add_argument('--endpoint', help='Bio-OS instance platform endpoint', default=DEFAULT_ENDPOINT)
    return parser


def handle(args) -> str:
    login_to_bioos(access_key=args.ak, secret_key=args.sk, endpoint=args.endpoint)
    workspace_id, _ = resolve_workspace(args.workspace_name)
    workflow_resource = WorkflowResource(workspace_id)
    df = workflow_resource.list()
    workflow_info = df[df.ID == args.workflow_id]

    if len(workflow_info) != 1:
        raise RuntimeError(f"Workflow with ID {args.workflow_id} not found")

    status = workflow_info.iloc[0]["Status"]["Phase"]
    message = workflow_info.iloc[0]["Status"].get("Message", "")
    lines = [f"Status: {status}"]
    if message:
        lines.append(f"Message: {message}")
    return "\n".join(lines)


def bioos_workflow_status_check():
    """Command line entry point for checking workflow validation status"""
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
