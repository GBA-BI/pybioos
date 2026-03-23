import sys

from bioos.cli.common import add_argument, build_parser, run_cli
from bioos.ops.auth import workspace_context_from_args


def build_args():
    parser = build_parser("List workflows from a Bio-OS workspace.")
    add_argument(parser, "workspace_name", required=True, help="Workspace name.")
    add_argument(parser, "search_keyword", required=False, default=None, help="Optional workflow keyword.")
    add_argument(parser, "page_number", required=False, type=int, default=1, help="Page number.")
    add_argument(parser, "page_size", required=False, type=int, default=10, help="Page size.")
    return parser


def handle(args):
    from bioos.service.api import list_workflows

    workspace_id, _ = workspace_context_from_args(args)
    return list_workflows(
        workspace_id=workspace_id,
        search_keyword=args.search_keyword,
        page_number=args.page_number,
        page_size=args.page_size,
    ) or []


def main():
    parser = build_args()
    args = parser.parse_args()
    sys.exit(run_cli(handle, args))


if __name__ == "__main__":
    main()
