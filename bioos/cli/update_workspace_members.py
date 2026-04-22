import sys

from bioos.cli.common import add_argument, build_parser, run_cli
from bioos.ops.auth import workspace_context_from_args


def build_args():
    parser = build_parser("Update workspace members.")
    add_argument(parser, "workspace_name", required=True, help="Workspace name.")
    add_argument(
        parser,
        "name",
        required=True,
        action="append",
        help="Member username. Can be specified multiple times.",
    )
    add_argument(
        parser,
        "role",
        required=True,
        help="Workspace member role: Visitor, User, or Admin.",
    )
    return parser


def handle(args):
    _, ws = workspace_context_from_args(args)
    result = ws.update_members(names=args.name, role=args.role)
    return {
        "success": True,
        "workspace_name": args.workspace_name,
        "role": args.role,
        "names": args.name,
        "result": result,
    }


def main():
    parser = build_args()
    args = parser.parse_args()
    sys.exit(run_cli(handle, args))


if __name__ == "__main__":
    main()
