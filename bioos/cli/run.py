from bioos.ops.auth import login_with_args, resolve_workspace
from bioos.resource.workflows import Run


def handle_list(args):
    workspace_id = _login_and_resolve_workspace(args)
    return Run.list_runs(
        workspace_id=workspace_id,
        submission_id=args.submission_id,
        page_number=args.page_number,
        page_size=args.page_size,
        filter_=_build_list_filter(args),
    )


def handle_tasks(args):
    workspace_id = _login_and_resolve_workspace(args)
    return Run.list_tasks(
        workspace_id=workspace_id,
        run_id=args.run_id,
        page_number=args.page_number,
        page_size=args.page_size,
    )


def handle_metric_data(args):
    workspace_id = _login_and_resolve_workspace(args)
    return Run.get_task_metric_data_for_run(
        workspace_id=workspace_id,
        run_id=args.run_id,
        name=args.task_name,
        period=args.period,
        start_time=args.start_time,
        end_time=args.end_time,
    )


def _login_and_resolve_workspace(args) -> str:
    login_with_args(args)
    workspace_id, _ = resolve_workspace(args.workspace_name)
    return workspace_id


def _build_list_filter(args):
    filter_ = {}
    if args.keyword:
        filter_["Keyword"] = args.keyword
    if args.run_id:
        filter_["IDs"] = args.run_id
    if args.status:
        filter_["Status"] = args.status
    return filter_ or None
