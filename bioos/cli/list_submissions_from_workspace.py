import sys

from bioos.cli.common import add_argument, build_parser, run_cli
from bioos.ops.auth import workspace_context_from_args


def build_args():
    parser = build_parser("List submissions from a Bio-OS workspace.")
    add_argument(parser, "workspace_name", required=True, help="Workspace name.")
    add_argument(parser, "workflow_name", required=False, default=None, help="Optional workflow name filter.")
    add_argument(parser, "search_keyword", required=False, default=None, help="Optional submission keyword.")
    add_argument(parser, "status", required=False, default=None, help="Optional submission status filter.")
    add_argument(parser, "page_number", required=False, type=int, default=1, help="Page number.")
    add_argument(parser, "page_size", required=False, type=int, default=10, help="Page size.")
    return parser


def handle(args):
    from bioos.service.api import list_submissions

    workspace_id, _ = workspace_context_from_args(args)
    return list_submissions(
        workspace_id=workspace_id,
        workflow_name=args.workflow_name,
        search_keyword=args.search_keyword,
        status=args.status,
        page_number=args.page_number,
        page_size=args.page_size,
    ) or []


def main():
    parser = build_args()
    args = parser.parse_args()
    sys.exit(run_cli(handle, args))


if __name__ == "__main__":
    main()
