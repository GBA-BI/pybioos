import sys

from bioos.cli.common import add_argument, build_parser, run_cli
from bioos.ops.auth import workspace_context_from_args
from bioos.ops.formatters import dataframe_records


def build_args():
    parser = build_parser("List IES application instances in a Bio-OS workspace.")
    add_argument(parser, "workspace_name", required=True, help="Workspace name.")
    return parser


def handle(args):
    _, ws = workspace_context_from_args(args)
    ies_df = ws.webinstanceapps.list()
    return dataframe_records(ies_df)


def main():
    parser = build_args()
    args = parser.parse_args()
    sys.exit(run_cli(handle, args))


if __name__ == "__main__":
    main()
