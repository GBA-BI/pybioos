import sys

from bioos.cli.common import add_argument, build_parser, run_cli
from bioos.ops.auth import workspace_context_from_args


def build_args():
    parser = build_parser("Delete a submission from a workspace.")
    add_argument(parser, "workspace_name", required=True, help="Workspace name.")
    add_argument(parser, "submission_id", required=True, help="Submission ID to delete.")
    return parser


def handle(args):
    from bioos.resource.workflows import Submission

    workspace_id, _ = workspace_context_from_args(args)
    submission = Submission(workspace_id, args.submission_id)
    submission.delete()
    return {
        "success": True,
        "workspace_name": args.workspace_name,
        "submission_id": args.submission_id,
        "message": f"Submission '{args.submission_id}' deleted.",
    }


def main():
    parser = build_args()
    args = parser.parse_args()
    sys.exit(run_cli(handle, args))


if __name__ == "__main__":
    main()

