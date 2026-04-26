import sys

from bioos.cli.common import add_argument, add_bool_argument, build_parser, run_cli
from bioos.ops.auth import workspace_context_from_args


def build_args():
    parser = build_parser("List workspace members.")
    add_argument(parser, "workspace_name", required=True, help="Workspace name.")
    add_argument(parser, "page_number", required=False, type=int, help="Page number.")
    add_argument(parser, "page_size", required=False, type=int, help="Page size.")
    add_bool_argument(
        parser,
        "in_workspace",
        default=True,
        help_text="Only list users already in the workspace.",
    )
    add_argument(
        parser,
        "role",
        required=False,
        action="append",
        help="Optional member role filter. Can be specified multiple times.",
    )
    add_argument(
        parser,
        "keyword",
        required=False,
        help="Optional username keyword filter.",
    )
    return parser


def handle(args):
    _, ws = workspace_context_from_args(args)
    members = ws.list_members(
        page_number=args.page_number,
        page_size=args.page_size,
        in_workspace=args.in_workspace,
        roles=args.role,
        keyword=args.keyword,
    )
    return {
        "success": True,
        "workspace_name": args.workspace_name,
        "in_workspace": args.in_workspace,
        "members": members,
    }


def main():
    parser = build_args()
    args = parser.parse_args()
    sys.exit(run_cli(handle, args))


if __name__ == "__main__":
    main()
