import sys

from bioos.cli.common import add_argument, add_bool_argument, build_parser, run_cli
from bioos.ops.auth import workspace_context_from_args
from bioos.ops.formatters import dataframe_records


def build_args():
    parser = build_parser("List files from a Bio-OS workspace.")
    add_argument(parser, "workspace_name", required=True, help="Workspace name.")
    add_argument(parser, "prefix", required=False, default="", help="Prefix path to list.")
    add_bool_argument(parser, "recursive", default=False, help_text="List files recursively.")
    return parser


def handle(args):
    _, ws = workspace_context_from_args(args)
    files_df = ws.files.list(prefix=args.prefix, recursive=args.recursive)
    return dataframe_records(files_df)


def main():
    parser = build_args()
    args = parser.parse_args()
    sys.exit(run_cli(handle, args))


if __name__ == "__main__":
    main()

