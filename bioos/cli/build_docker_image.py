import sys

from bioos.cli.common import add_argument, build_parser, run_cli
from bioos.ops.docker_build import build_docker_image_request


def build_args():
    parser = build_parser("Submit a Docker image build request.", include_auth=False)
    add_argument(parser, "repo_name", required=True, help="Repository name.")
    add_argument(parser, "tag", required=True, help="Image tag.")
    add_argument(parser, "source_path", required=True, help="Path to Dockerfile or zip archive.")
    add_argument(parser, "registry", required=False, default="registry-vpc.miracle.ac.cn", help="Registry.")
    add_argument(parser, "namespace_name", required=False, default="auto-build", help="Registry namespace.")
    return parser


def handle(args):
    return build_docker_image_request(
        repo_name=args.repo_name,
        tag=args.tag,
        source_path=args.source_path,
        registry=args.registry,
        namespace_name=args.namespace_name,
    )


def main():
    parser = build_args()
    args = parser.parse_args()
    sys.exit(run_cli(handle, args))


if __name__ == "__main__":
    main()

