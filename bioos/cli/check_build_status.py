import sys

from bioos.cli.common import add_argument, build_parser, run_cli
from bioos.ops.docker_build import check_build_status_request


def build_args():
    parser = build_parser("Check Docker image build status.", include_auth=False)
    add_argument(parser, "task_id", required=True, help="Build task ID.")
    return parser


def handle(args):
    return check_build_status_request(args.task_id)


def main():
    parser = build_args()
    args = parser.parse_args()
    sys.exit(run_cli(handle, args))


if __name__ == "__main__":
    main()

