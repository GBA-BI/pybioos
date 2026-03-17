import sys

from bioos.cli.common import add_argument, build_parser, run_cli
from bioos.ops.auth import login_with_args
from bioos.ops.formatters import dataframe_records


def build_args():
    parser = build_parser("List Bio-OS workspaces.")
    add_argument(
        parser,
        "page_size",
        required=False,
        type=int,
        default=None,
        help="Maximum number of workspaces to return. This is applied locally after fetching.",
    )
    return parser


def handle(args):
    from bioos import bioos

    login_with_args(args)
    workspaces = bioos.list_workspaces()
    if getattr(workspaces, "empty", False):
        return []

    try:
        selected = workspaces[["Name", "Description"]]
    except Exception:
        selected = workspaces

    records = dataframe_records(selected)
    if args.page_size is not None:
        records = records[: args.page_size]
    return records


def main():
    parser = build_args()
    args = parser.parse_args()
    sys.exit(run_cli(handle, args))


if __name__ == "__main__":
    main()
