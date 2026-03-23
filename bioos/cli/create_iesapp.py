import sys

from bioos.cli.common import add_argument, add_bool_argument, build_parser, run_cli
from bioos.ops.auth import workspace_context_from_args


def build_args():
    parser = build_parser("Create a new IES application instance.")
    add_argument(parser, "workspace_name", required=True, help="Workspace name.")
    add_argument(parser, "ies_name", required=True, help="IES instance name.")
    add_argument(parser, "ies_desc", required=True, help="IES instance description.")
    add_argument(parser, "ies_resource", required=False, default="2c-4gib", help="Resource size.")
    add_argument(
        parser,
        "ies_storage",
        required=False,
        type=int,
        default=42949672960,
        help="Storage capacity in bytes.",
    )
    add_argument(
        parser,
        "ies_image",
        required=False,
        default="registry-vpc.miracle.ac.cn/infcprelease/ies-default:v0.0.14",
        help="Docker image URL.",
    )
    add_bool_argument(parser, "ies_ssh", default=True, help_text="Enable SSH.")
    add_argument(
        parser,
        "ies_run_limit",
        required=False,
        type=int,
        default=10800,
        help="Maximum running time in seconds.",
    )
    add_argument(
        parser,
        "ies_idle_timeout",
        required=False,
        type=int,
        default=10800,
        help="Idle timeout in seconds.",
    )
    add_bool_argument(parser, "ies_auto_start", default=True, help_text="Auto-start the instance.")
    return parser


def handle(args):
    _, ws = workspace_context_from_args(args)
    result = ws.webinstanceapps.create_new_instance(
        name=args.ies_name,
        description=args.ies_desc,
        resource_size=args.ies_resource,
        storage_capacity=args.ies_storage,
        image=args.ies_image,
        ssh_enabled=args.ies_ssh,
        running_time_limit_seconds=args.ies_run_limit,
        idle_timeout_seconds=args.ies_idle_timeout,
        auto_start=args.ies_auto_start,
    )
    return {
        "success": True,
        "workspace_name": args.workspace_name,
        "ies_name": args.ies_name,
        "result": result,
    }


def main():
    parser = build_args()
    args = parser.parse_args()
    sys.exit(run_cli(handle, args))


if __name__ == "__main__":
    main()

