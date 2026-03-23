import argparse
import logging
import sys
import time

from bioos import bioos
from bioos.config import DEFAULT_ENDPOINT
from bioos.ops.auth import login_to_bioos, resolve_workspace
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


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description='Bio-OS Workflow Import Tool',
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument('--ak', required=False, help='Access key for your Bio-OS instance platform account')
    parser.add_argument('--sk', required=False, help='Secret key for your Bio-OS instance platform account')
    parser.add_argument('--workspace_name', required=True, help='Target workspace name')
    parser.add_argument('--workflow_name', required=True, help='Name for the workflow to be imported')
    parser.add_argument('--workflow_source', required=True, help='Local WDL file path or git repository URL')
    parser.add_argument('--endpoint', help='Bio-OS instance platform endpoint', default=DEFAULT_ENDPOINT)
    parser.add_argument('--workflow_desc', help='Description for the workflow', default='')
    parser.add_argument('--main_path', help='Main workflow file path (required for git repository)', default='')
    parser.add_argument('--monitor', action='store_true', help='Monitor the workflow validation status until completion')
    parser.add_argument('--monitor_interval', type=int, default=60, help='Time interval in seconds for checking workflow status')
    return parser


def handle(args) -> str:
    logger = get_logger()
    login_to_bioos(access_key=args.ak, secret_key=args.sk, endpoint=args.endpoint)
    workspace_id, _ = resolve_workspace(args.workspace_name)
    workflow_resource = WorkflowResource(workspace_id)
    result = workflow_resource.import_workflow(
        source=args.workflow_source,
        name=args.workflow_name,
        description=args.workflow_desc,
        language="WDL",
        main_workflow_path=args.main_path,
    )
    logger.info(f"Successfully uploaded workflow: {result}, validating..., please wait...")

    if not args.monitor:
        return f"Workflow {args.workflow_name} is still validating, {result}, please wait and check the status later."

    max_retries = 10
    retry_count = 0
    while retry_count < max_retries:
        df = workflow_resource.list()
        workflow_info = df[df.Name == args.workflow_name]
        if len(workflow_info) != 1:
            raise RuntimeError(f"Workflow {args.workflow_name} not found after import")

        status = workflow_info.iloc[0]["Status"]["Phase"]
        if status == "Succeeded":
            return f"Workflow {args.workflow_name} validated successfully"
        if status == "Failed":
            raise RuntimeError(f"Workflow {args.workflow_name} validation failed")
        if status != "Importing":
            raise RuntimeError(f"Workflow {args.workflow_name} has unknown status: {status}")

        logger.info(f"Workflow {args.workflow_name} is still validating, please wait...")
        time.sleep(args.monitor_interval)
        retry_count += 1

    raise RuntimeError(f"Workflow validation timeout after {max_retries} retries")


def bioos_workflow_import():
    """Command line entry point"""
    parser = build_parser()
    args = parser.parse_args()
    logger = get_logger()
    try:
        message = handle(args)
        if message:
            print(message)
        sys.exit(0)
    except Exception as exc:
        logger.error(f"Error: {exc}")
        sys.exit(1)


if __name__ == '__main__':
    bioos_workflow_import()
