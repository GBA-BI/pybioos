import sys

from bioos.cli.common import add_argument, build_parser, run_cli
from bioos.ops.auth import workspace_context_from_args


def build_args():
    parser = build_parser("Delete members from a workspace.")
    add_argument(parser, "workspace_name", required=True, help="Workspace name.")
    add_argument(
        parser,
        "name",
        required=False,
        action="append",
        help="Member username. Can be specified multiple times.",
    )
    return parser


def handle(args):
    _, ws = workspace_context_from_args(args)
    result = ws.delete_members(names=args.name)
    return {
        "success": True,
        "workspace_name": args.workspace_name,
        "names": args.name,
        "result": result,
    }


def main():
    parser = build_args()
    args = parser.parse_args()
    sys.exit(run_cli(handle, args))


if __name__ == "__main__":
    main()
