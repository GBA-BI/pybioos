import sys

from bioos.cli.common import add_argument, build_parser, run_cli
from bioos.ops.auth import workspace_context_from_args


def build_args():
    parser = build_parser("Generate the inputs template for a registered workflow.")
    add_argument(parser, "workspace_name", required=True, help="Workspace name.")
    add_argument(parser, "workflow_name", required=True, help="Workflow name.")
    return parser


def handle(args):
    _, ws = workspace_context_from_args(args)
    workflow = ws.workflow(args.workflow_name)
    return workflow.get_input_template()


def main():
    parser = build_args()
    args = parser.parse_args()
    sys.exit(run_cli(handle, args))


if __name__ == "__main__":
    main()

