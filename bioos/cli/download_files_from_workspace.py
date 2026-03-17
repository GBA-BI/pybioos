import sys

from bioos.cli.common import add_argument, add_bool_argument, build_parser, run_cli
from bioos.ops.auth import workspace_context_from_args


def build_args():
    parser = build_parser("Download files from a Bio-OS workspace.")
    add_argument(parser, "workspace_name", required=True, help="Workspace name.")
    add_argument(
        parser,
        "source",
        required=True,
        action="append",
        help="Source file path in the workspace. Can be specified multiple times.",
    )
    add_argument(parser, "target", required=True, help="Local target path.")
    add_bool_argument(parser, "flatten", default=False, help_text="Flatten directories during download.")
    return parser


def handle(args):
    _, ws = workspace_context_from_args(args)
    sources = args.source[0] if len(args.source) == 1 else args.source
    success = ws.files.download(sources=sources, target=args.target, flatten=args.flatten)
    return {
        "success": success,
        "workspace_name": args.workspace_name,
        "sources": args.source,
        "target": args.target,
        "flatten": args.flatten,
    }


def main():
    parser = build_args()
    args = parser.parse_args()
    sys.exit(run_cli(handle, args))


if __name__ == "__main__":
    main()

