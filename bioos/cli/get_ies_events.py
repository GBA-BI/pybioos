import sys

from bioos.cli.common import add_argument, build_parser, run_cli
from bioos.ops.auth import workspace_context_from_args


def build_args():
    parser = build_parser("Get IES events.")
    add_argument(parser, "workspace_name", required=True, help="Workspace name.")
    add_argument(parser, "ies_name", required=True, help="IES instance name.")
    return parser


def handle(args):
    _, ws = workspace_context_from_args(args)
    events = ws.webinstanceapps.get_events(args.ies_name)
    return {
        "workspace_name": args.workspace_name,
        "ies_name": args.ies_name,
        "events": events,
    }


def main():
    parser = build_args()
    args = parser.parse_args()
    sys.exit(run_cli(handle, args))


if __name__ == "__main__":
    main()

