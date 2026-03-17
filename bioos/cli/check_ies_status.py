import sys

from bioos.cli.common import add_argument, build_parser, run_cli
from bioos.ops.auth import workspace_context_from_args
from bioos.ops.formatters import dataframe_records


def build_args():
    parser = build_parser("Check IES instance status.")
    add_argument(parser, "workspace_name", required=True, help="Workspace name.")
    add_argument(parser, "ies_name", required=True, help="IES instance name.")
    return parser


def handle(args):
    _, ws = workspace_context_from_args(args)
    app = ws.webinstanceapp(args.ies_name)
    list_df = ws.webinstanceapps.list().query(f"Name=='{args.ies_name}'")
    list_records = dataframe_records(list_df)
    return {
        "name": args.ies_name,
        "status": app.status,
        "status_detail": app.status_detail,
        "access_urls": app.access_urls,
        "endpoint": app.endpoint,
        "resource_size": app.resource_size,
        "storage_capacity": app.storage_capacity,
        "ssh_info": app.ssh_info,
        "list_record": list_records[0] if list_records else None,
    }


def main():
    parser = build_args()
    args = parser.parse_args()
    sys.exit(run_cli(handle, args))


if __name__ == "__main__":
    main()

