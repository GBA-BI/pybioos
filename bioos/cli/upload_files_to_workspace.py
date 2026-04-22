import sys

from bioos.cli.common import add_argument, add_bool_argument, build_parser, run_cli
from bioos.ops.workspace_files import upload_local_files_to_workspace


def build_args():
    parser = build_parser("Upload local files to a Bio-OS workspace.")
    add_argument(parser, "workspace_name", required=True, help="Workspace name.")
    add_argument(
        parser,
        "source",
        required=True,
        action="append",
        help="Local file path. Can be specified multiple times.",
    )
    add_argument(parser, "target", required=False, default="", help="Target prefix path in the workspace bucket.")
    add_bool_argument(parser, "flatten", default=True, help_text="Flatten local paths during upload.")
    add_bool_argument(parser, "skip_existing", default=False, help_text="Skip files whose target object already exists.")
    add_argument(
        parser,
        "checkpoint_dir",
        required=False,
        default=None,
        help="Directory for resumable upload checkpoints.",
    )
    add_argument(
        parser,
        "max_retries",
        required=False,
        type=int,
        default=3,
        help="Number of retries per file after the initial attempt.",
    )
    add_argument(
        parser,
        "task_num",
        required=False,
        type=int,
        default=10,
        help="Parallel task count for multipart uploads.",
    )
    return parser


def handle(args):
    return upload_local_files_to_workspace(
        workspace_name=args.workspace_name,
        sources=args.source,
        target=args.target,
        flatten=args.flatten,
        skip_existing=args.skip_existing,
        checkpoint_dir=args.checkpoint_dir,
        max_retries=args.max_retries,
        task_num=args.task_num,
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
