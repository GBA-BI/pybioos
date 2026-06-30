import sys

from bioos.cli.common import add_argument, build_parser, run_cli
from bioos.ops.auth import login_to_bioos, resolve_workspace
from bioos.resource.workflows import WorkflowResource


def build_args():
    parser = build_parser("Update a Bio-OS workflow.")
    add_argument(parser, "workspace_name", required=True, help="Workspace name.")
    add_argument(parser, "workflow_id", required=True, help="Workflow ID.")
    add_argument(parser, "workflow_name", required=True, help="Workflow name.")
    add_argument(parser, "workflow_desc", required=False, default=None, help="Workflow description.")
    add_argument(
        parser,
        "workflow_source",
        required=False,
        default="",
        help="Optional local .wdl file path or local directory containing WDL files.",
    )
    add_argument(
        parser,
        "main_path",
        required=False,
        default="",
        help="Main workflow file path for local WDL directory updates.",
    )
    return parser


def handle(args):
    login_to_bioos(access_key=args.ak, secret_key=args.sk, endpoint=args.endpoint)
    workspace_id, _ = resolve_workspace(args.workspace_name)
    resource = WorkflowResource(workspace_id)
    result = resource.update_workflow(
        workflow_id=args.workflow_id,
        name=args.workflow_name,
        description=args.workflow_desc,
        source=args.workflow_source,
        main_workflow_path=args.main_path,
    )
    return {
        "success": True,
        "workflow_id": args.workflow_id,
        "workflow_name": args.workflow_name,
        "result": result,
    }


def main():
    parser = build_args()
    args = parser.parse_args()
    sys.exit(run_cli(handle, args))


if __name__ == "__main__":
    main()
