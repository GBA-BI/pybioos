import sys

from bioos.cli.common import add_argument, build_parser, run_cli
from bioos.ops.docker_build import get_docker_image_url


def build_args():
    parser = build_parser("Build the full Docker image URL.", include_auth=False)
    add_argument(parser, "repo_name", required=True, help="Repository name.")
    add_argument(parser, "tag", required=True, help="Image tag.")
    add_argument(parser, "registry", required=False, default="registry-vpc.miracle.ac.cn", help="Registry.")
    add_argument(parser, "namespace_name", required=False, default="auto-build", help="Registry namespace.")
    return parser


def handle(args):
    return {"image_url": get_docker_image_url(args.registry, args.namespace_name, args.repo_name, args.tag)}


def main():
    parser = build_args()
    args = parser.parse_args()
    sys.exit(run_cli(handle, args))


if __name__ == "__main__":
    main()

