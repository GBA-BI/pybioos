import sys

from bioos.cli.common import add_argument, build_parser, run_cli
from bioos.ops.auth import login_with_args


def build_asset_usage_data_args():
    parser = build_parser("Get asset usage time-series data.")
    add_argument(parser, "start_time", required=True, type=int, help="Start timestamp at an exact hour.")
    add_argument(parser, "end_time", required=True, type=int, help="End timestamp at an exact hour.")
    add_argument(
        parser,
        "type",
        required=True,
        help="Asset usage type: WorkspaceVisit or WorkflowUse.",
    )
    return parser


def build_asset_usage_list_args():
    parser = build_parser("List asset usage records.")
    add_argument(parser, "start_time", required=True, type=int, help="Start timestamp at an exact hour.")
    add_argument(parser, "end_time", required=True, type=int, help="End timestamp at an exact hour.")
    add_argument(
        parser,
        "type",
        required=True,
        help="Asset usage type: WorkspaceVisit or WorkflowUse.",
    )
    return parser


def build_asset_usage_total_args():
    parser = build_parser("Get total asset usage.")
    add_argument(parser, "start_time", required=True, type=int, help="Start timestamp at an exact hour.")
    add_argument(parser, "end_time", required=True, type=int, help="End timestamp at an exact hour.")
    add_argument(
        parser,
        "type",
        required=True,
        help="Asset usage type: WorkspaceVisit or WorkflowUse.",
    )
    return parser


def build_resource_usage_data_args():
    parser = build_parser("Get resource usage time-series data.")
    add_argument(parser, "start_time", required=True, type=int, help="Start timestamp at an exact hour.")
    add_argument(parser, "end_time", required=True, type=int, help="End timestamp at an exact hour.")
    add_argument(
        parser,
        "type",
        required=True,
        help="Resource usage type: cpu, memory, storage, tos, or gpu.",
    )
    add_argument(
        parser,
        "sub_dimension",
        required=False,
        action="append",
        help="Optional sub-dimension. Can be specified multiple times.",
    )
    return parser


def build_workspace_resource_usage_args():
    parser = build_parser("List workspace resource usage.")
    add_argument(parser, "start_time", required=True, type=int, help="Start timestamp at an exact hour.")
    add_argument(parser, "end_time", required=True, type=int, help="End timestamp at an exact hour.")
    return parser


def build_user_resource_usage_args():
    parser = build_parser("List user resource usage.")
    add_argument(parser, "start_time", required=True, type=int, help="Start timestamp at an exact hour.")
    add_argument(parser, "end_time", required=True, type=int, help="End timestamp at an exact hour.")
    return parser


def build_total_resource_usage_args():
    parser = build_parser("Get total resource usage.")
    add_argument(parser, "start_time", required=True, type=int, help="Start timestamp at an exact hour.")
    add_argument(parser, "end_time", required=True, type=int, help="End timestamp at an exact hour.")
    return parser


def handle_asset_usage_data(args):
    from bioos import bioos

    login_with_args(args)
    result = bioos.usage().get_asset_usage_data(args.start_time, args.end_time, args.type)
    return {
        "success": True,
        "start_time": args.start_time,
        "end_time": args.end_time,
        "type": args.type,
        "result": result,
    }


def handle_asset_usage_list(args):
    from bioos import bioos

    login_with_args(args)
    result = bioos.usage().list_asset_usage(args.start_time, args.end_time, args.type)
    return {
        "success": True,
        "start_time": args.start_time,
        "end_time": args.end_time,
        "type": args.type,
        "result": result,
    }


def handle_asset_usage_total(args):
    from bioos import bioos

    login_with_args(args)
    result = bioos.usage().get_total_asset_usage(args.start_time, args.end_time, args.type)
    return {
        "success": True,
        "start_time": args.start_time,
        "end_time": args.end_time,
        "type": args.type,
        "result": result,
    }


def handle_resource_usage_data(args):
    from bioos import bioos

    login_with_args(args)
    result = bioos.usage().get_resource_usage_data(
        args.start_time,
        args.end_time,
        args.type,
        sub_dimensions=args.sub_dimension,
    )
    return {
        "success": True,
        "start_time": args.start_time,
        "end_time": args.end_time,
        "type": args.type,
        "sub_dimensions": args.sub_dimension,
        "result": result,
    }


def handle_workspace_resource_usage(args):
    from bioos import bioos

    login_with_args(args)
    result = bioos.usage().list_workspace_resource_usage(args.start_time, args.end_time)
    return {
        "success": True,
        "start_time": args.start_time,
        "end_time": args.end_time,
        "result": result,
    }


def handle_user_resource_usage(args):
    from bioos import bioos

    login_with_args(args)
    result = bioos.usage().list_user_resource_usage(args.start_time, args.end_time)
    return {
        "success": True,
        "start_time": args.start_time,
        "end_time": args.end_time,
        "result": result,
    }


def handle_total_resource_usage(args):
    from bioos import bioos

    login_with_args(args)
    result = bioos.usage().get_total_resource_usage(args.start_time, args.end_time)
    return {
        "success": True,
        "start_time": args.start_time,
        "end_time": args.end_time,
        "result": result,
    }


def main():
    parser = build_asset_usage_data_args()
    args = parser.parse_args()
    sys.exit(run_cli(handle_asset_usage_data, args))


if __name__ == "__main__":
    main()
