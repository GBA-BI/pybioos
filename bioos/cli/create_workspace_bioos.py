import sys

from bioos.cli.common import add_argument, build_parser, run_cli
from bioos.ops.auth import login_with_args


def build_args():
    parser = build_parser("Create a Bio-OS workspace and bind default clusters.")
    add_argument(parser, "workspace_name", required=True, help="Workspace name to create.")
    add_argument(
        parser,
        "workspace_description",
        required=True,
        help="Workspace description.",
    )
    return parser


def handle(args):
    from bioos import bioos

    login_with_args(args)
    result = bioos.create_workspace(name=args.workspace_name, description=args.workspace_description)
    workspace_id = result.get("ID")
    if not workspace_id:
        raise RuntimeError(f"Workspace creation failed: {result}")

    ws = bioos.workspace(workspace_id)
    workflow_bind_result = ws.bind_cluster(cluster_id="default", type_="workflow")
    ies_bind_result = ws.bind_cluster(cluster_id="default", type_="webapp-ies")
    return {
        "success": True,
        "workspace_id": workspace_id,
        "workspace_name": args.workspace_name,
        "create_result": result,
        "workflow_bind_result": workflow_bind_result,
        "ies_bind_result": ies_bind_result,
    }


def main():
    parser = build_args()
    args = parser.parse_args()
    sys.exit(run_cli(handle, args))


if __name__ == "__main__":
    main()
