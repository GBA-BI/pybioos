import sys

from bioos.cli.common import add_argument, add_bool_argument, build_parser, run_cli
from bioos.ops.dockstore import search_dockstore_workflows


def build_args():
    parser = build_parser("Search workflows from Dockstore.", include_auth=False)
    parser.add_argument(
        "--query",
        action="append",
        nargs=3,
        metavar=("FIELD", "OPERATOR", "TERM"),
        required=True,
        help="Query tuple: field operator term. Can be repeated.",
    )
    add_argument(parser, "top_n", required=False, type=int, default=3, help="Number of top results to return.")
    add_argument(
        parser,
        "query_type",
        required=False,
        default="match_phrase",
        choices=("match_phrase", "wildcard"),
        help="Search query type.",
    )
    add_bool_argument(parser, "sentence", default=False, help_text="Treat search terms as sentence queries.")
    add_bool_argument(parser, "output_full", default=False, help_text="Include more workflow metadata.")
    return parser


def handle(args):
    return search_dockstore_workflows(
        query=args.query,
        top_n=args.top_n,
        query_type=args.query_type,
        sentence=args.sentence,
        output_full=args.output_full,
    )


def main():
    parser = build_args()
    args = parser.parse_args()
    sys.exit(run_cli(handle, args))


if __name__ == "__main__":
    main()

