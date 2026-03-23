import sys

from bioos.cli.common import add_argument, build_parser, run_cli
from bioos.ops.womtool import validate_wdl_file


def build_args():
    parser = build_parser("Validate a WDL file with womtool.", include_auth=False)
    add_argument(parser, "wdl_path", required=True, help="Path to the WDL file.")
    return parser


def handle(args):
    return validate_wdl_file(args.wdl_path)


def main():
    parser = build_args()
    args = parser.parse_args()
    sys.exit(run_cli(handle, args))


if __name__ == "__main__":
    main()

