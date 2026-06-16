import sys

from bioos.cli.common import add_argument, build_parser, run_cli
from bioos.ops.auth import login_with_args


def build_args():
    parser = build_parser("Delete a Bio-OS workspace.")
    add_argument(parser, "workspace_name", required=True, help="Workspace name or ID.")
    return parser


def handle(args):
    from bioos import bioos

    login_with_args(args)
    result = bioos.workspace(args.workspace_name).delete()
    return {
        "success": True,
        "workspace_name": args.workspace_name,
        "result": result,
    }


def main():
    parser = build_args()
    args = parser.parse_args()
    sys.exit(run_cli(handle, args))


if __name__ == "__main__":
    main()
