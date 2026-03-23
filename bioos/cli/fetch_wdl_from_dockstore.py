import sys

from bioos.cli.common import add_argument, build_parser, run_cli
from bioos.ops.dockstore import fetch_wdl_from_dockstore_url


def build_args():
    parser = build_parser("Download workflow files from Dockstore.", include_auth=False)
    add_argument(parser, "url", required=True, help="Dockstore workflow URL or path.")
    add_argument(parser, "output_path", required=False, default=".", help="Output directory.")
    return parser


def handle(args):
    return fetch_wdl_from_dockstore_url(args.url, args.output_path)


def main():
    parser = build_args()
    args = parser.parse_args()
    sys.exit(run_cli(handle, args))


if __name__ == "__main__":
    main()

