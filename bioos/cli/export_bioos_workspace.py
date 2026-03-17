import sys

from bioos.cli.common import add_argument, build_parser, run_cli
from bioos.ops.auth import workspace_context_from_args


def build_args():
    parser = build_parser("Export workspace metadata to a local path.")
    add_argument(parser, "workspace_name", required=True, help="Workspace name.")
    add_argument(parser, "export_path", required=True, help="Local export path.")
    return parser


def handle(args):
    _, ws = workspace_context_from_args(args)
    result = ws.export_workspace_v2(
        download_path=args.export_path,
        monitor=True,
        monitor_interval=5,
        max_retries=60,
    )
    return {
        "success": True,
        "workspace_name": args.workspace_name,
        "export_path": args.export_path,
        "result": result,
    }


def main():
    parser = build_args()
    args = parser.parse_args()
    sys.exit(run_cli(handle, args))


if __name__ == "__main__":
    main()

