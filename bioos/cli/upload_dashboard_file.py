import sys

from bioos.cli.common import add_argument, build_parser, run_cli
from bioos.ops.workspace_files import upload_dashboard_file_to_workspace


def build_args():
    parser = build_parser("Upload __dashboard__.md to the root of a workspace bucket.")
    add_argument(parser, "workspace_name", required=True, help="Workspace name.")
    add_argument(parser, "local_file_path", required=True, help="Path to __dashboard__.md.")
    return parser


def handle(args):
    return upload_dashboard_file_to_workspace(
        workspace_name=args.workspace_name,
        local_file_path=args.local_file_path,
        access_key=args.ak,
        secret_key=args.sk,
        endpoint=args.endpoint,
    )


def main():
    parser = build_args()
    args = parser.parse_args()
    sys.exit(run_cli(handle, args))


if __name__ == "__main__":
    main()

