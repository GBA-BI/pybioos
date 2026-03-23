import sys

from bioos.cli.common import add_argument, add_bool_argument, build_parser, run_cli
from bioos.ops.workspace_profile import WorkspaceProfileOptions, get_workspace_profile_data

DEFAULT_ENDPOINT = "https://bio-top.miracle.ac.cn"


def build_args():
    parser = build_parser("Get a high-level Bio-OS workspace profile.")
    add_argument(parser, "workspace_name", required=True, help="Workspace name.")
    add_argument(
        parser,
        "submission_limit",
        required=False,
        type=int,
        default=5,
        help="Number of recent submissions to include.",
    )
    add_argument(
        parser,
        "artifact_limit_per_submission",
        required=False,
        type=int,
        default=10,
        help="Number of artifact samples to keep per submission.",
    )
    add_argument(
        parser,
        "sample_rows_per_data_model",
        required=False,
        type=int,
        default=3,
        help="Number of sample rows to include per data model.",
    )
    add_bool_argument(parser, "include_artifacts", default=True, help_text="Include artifact summaries.")
    add_bool_argument(
        parser,
        "include_failure_details",
        default=True,
        help_text="Include run-level failure summaries.",
    )
    add_bool_argument(parser, "include_ies", default=True, help_text="Include IES information.")
    add_bool_argument(
        parser,
        "include_signed_urls",
        default=False,
        help_text="Include signed file URLs in artifact summaries.",
    )
    parser.set_defaults(endpoint=DEFAULT_ENDPOINT)
    return parser


def handle(args):
    options = WorkspaceProfileOptions(
        workspace_name=args.workspace_name,
        submission_limit=args.submission_limit,
        artifact_limit_per_submission=args.artifact_limit_per_submission,
        sample_rows_per_data_model=args.sample_rows_per_data_model,
        include_artifacts=args.include_artifacts,
        include_failure_details=args.include_failure_details,
        include_ies=args.include_ies,
        include_signed_urls=args.include_signed_urls,
        endpoint=args.endpoint,
        access_key=args.ak,
        secret_key=args.sk,
    )
    return get_workspace_profile_data(options)


def main():
    parser = build_args()
    args = parser.parse_args()
    sys.exit(run_cli(handle, args))


if __name__ == "__main__":
    main()
