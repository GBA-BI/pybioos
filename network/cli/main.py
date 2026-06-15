import argparse
import json
import sys
from datetime import date, datetime
from pathlib import Path
from typing import Any, Iterable, Optional

import network
from bioos.config import DEFAULT_ENDPOINT
from bioos.ops.auth import resolve_auth_settings, resolve_credentials


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="network",
        description="BioOS Network command line interface.",
    )
    subparsers = parser.add_subparsers(dest="command")
    _add_library_group(subparsers)
    _add_dataset_group(subparsers)
    _add_drs_group(subparsers)
    return parser


def main(argv: Optional[Iterable[str]] = None) -> int:
    parser = build_parser()
    args = parser.parse_args(list(argv) if argv is not None else None)
    handler = getattr(args, "handler", None)
    if handler is None:
        current_parser = getattr(args, "_parser", parser)
        current_parser.print_help()
        return 0
    return run_cli(handler, args)


def _add_library_group(subparsers: Any) -> None:
    library_parser = subparsers.add_parser("library", help="Data library commands.")
    library_subparsers = library_parser.add_subparsers(dest="library_command")
    library_parser.set_defaults(_parser=library_parser)

    list_parser = library_subparsers.add_parser("list", help="List data libraries.")
    add_auth_arguments(list_parser)
    add_network_arguments(list_parser)
    add_output_arguments(list_parser)
    add_argument(list_parser, "page", required=False, type=int, help="Page number.")
    add_argument(list_parser, "size", required=False, type=int, help="Page size.")
    add_argument(list_parser, "order_by", required=False, help="Order expression.")
    add_argument(list_parser, "id", required=False, action="append", help="Data library ID. Can be repeated.")
    add_argument(list_parser, "display_name", required=False, action="append", help="Display name. Can be repeated.")
    add_argument(list_parser, "organization_id", required=False, action="append", help="Organization ID. Can be repeated.")
    add_bool_argument(list_parser, "mine", default=False, help_text="List data libraries associated with the current user.")
    list_parser.set_defaults(_parser=list_parser, handler=handle_library_list)

    get_parser = library_subparsers.add_parser("get", help="Get one data library.")
    add_auth_arguments(get_parser)
    add_network_arguments(get_parser)
    add_output_arguments(get_parser)
    add_argument(get_parser, "data_library_id", required=True, help="Data library ID.")
    get_parser.set_defaults(_parser=get_parser, handler=handle_library_get)

    dataset_parser = library_subparsers.add_parser("dataset", help="Data sets under one data library.")
    dataset_subparsers = dataset_parser.add_subparsers(dest="library_dataset_command")
    dataset_parser.set_defaults(_parser=dataset_parser)
    _add_dataset_subcommands(dataset_subparsers, library_scoped=True)


def _add_dataset_group(subparsers: Any) -> None:
    dataset_parser = subparsers.add_parser("dataset", help="Repository data set commands.")
    dataset_subparsers = dataset_parser.add_subparsers(dest="dataset_command")
    dataset_parser.set_defaults(_parser=dataset_parser)
    _add_dataset_subcommands(dataset_subparsers, library_scoped=False)


def _add_dataset_subcommands(dataset_subparsers: Any, library_scoped: bool) -> None:
    list_parser = dataset_subparsers.add_parser("list", help="List data sets.")
    add_auth_arguments(list_parser)
    add_network_arguments(list_parser)
    add_output_arguments(list_parser)
    add_dataset_list_arguments(list_parser, library_scoped=library_scoped)
    list_parser.set_defaults(_parser=list_parser)
    list_parser.set_defaults(handler=handle_library_dataset_list if library_scoped else handle_dataset_list)

    get_parser = dataset_subparsers.add_parser("get", help="Get one data set.")
    add_auth_arguments(get_parser)
    add_network_arguments(get_parser)
    add_output_arguments(get_parser)
    add_dataset_get_arguments(get_parser, library_scoped=library_scoped, include_user_filter=True)
    get_parser.set_defaults(_parser=get_parser)
    get_parser.set_defaults(handler=handle_library_dataset_get if library_scoped else handle_dataset_get)

    files_parser = dataset_subparsers.add_parser("files", help="List files under a data set.")
    add_auth_arguments(files_parser)
    add_network_arguments(files_parser)
    add_output_arguments(files_parser)
    add_dataset_files_arguments(files_parser, library_scoped=library_scoped)
    files_parser.set_defaults(_parser=files_parser)
    files_parser.set_defaults(handler=handle_library_dataset_files if library_scoped else handle_dataset_files)

    file_ids_parser = dataset_subparsers.add_parser("file-ids", help="List file IDs under a data set.")
    add_auth_arguments(file_ids_parser)
    add_network_arguments(file_ids_parser)
    add_output_arguments(file_ids_parser)
    add_dataset_file_ids_arguments(file_ids_parser, library_scoped=library_scoped)
    file_ids_parser.set_defaults(_parser=file_ids_parser)
    file_ids_parser.set_defaults(handler=handle_library_dataset_file_ids if library_scoped else handle_dataset_file_ids)

    download_files_parser = dataset_subparsers.add_parser("download-files", help="Download files under a data set.")
    add_auth_arguments(download_files_parser)
    add_network_arguments(download_files_parser)
    add_output_arguments(download_files_parser)
    add_dataset_download_files_arguments(download_files_parser, library_scoped=library_scoped)
    download_files_parser.set_defaults(_parser=download_files_parser)
    download_files_parser.set_defaults(
        handler=handle_library_dataset_download_files if library_scoped else handle_dataset_download_files
    )


def _add_drs_group(subparsers: Any) -> None:
    drs_parser = subparsers.add_parser("drs", help="GA4GH DRS object commands.")
    drs_parser.set_defaults(_parser=drs_parser)

    drs_subparsers = drs_parser.add_subparsers(dest="drs_command")

    get_parser = drs_subparsers.add_parser("get", help="Get GA4GH DRS object information.")
    add_auth_arguments(get_parser)
    add_network_arguments(get_parser)
    add_output_arguments(get_parser)
    add_drs_arguments(get_parser)
    get_parser.set_defaults(_parser=get_parser, handler=handle_drs)

    access_parser = drs_subparsers.add_parser("access", help="Get a GA4GH DRS object access URL.")
    add_auth_arguments(access_parser)
    add_network_arguments(access_parser)
    add_output_arguments(access_parser)
    add_drs_access_arguments(access_parser)
    access_parser.set_defaults(_parser=access_parser, handler=handle_drs_access)

    download_parser = drs_subparsers.add_parser("download", help="Download a GA4GH DRS object.")
    add_auth_arguments(download_parser)
    add_network_arguments(download_parser)
    add_output_arguments(download_parser)
    add_drs_download_arguments(download_parser)
    download_parser.set_defaults(_parser=download_parser, handler=handle_drs_download)

    locate_parser = drs_subparsers.add_parser("locate", help="Locate a DRS path in the repository.")
    add_auth_arguments(locate_parser)
    add_network_arguments(locate_parser)
    add_output_arguments(locate_parser)
    add_drs_locate_arguments(locate_parser)
    locate_parser.set_defaults(_parser=locate_parser, handler=handle_drs_locate)


def add_auth_arguments(parser: argparse.ArgumentParser) -> None:
    auth_group = parser.add_argument_group("Authentication")
    add_argument(auth_group, "ak", required=False, help="BioOS access key for bridge login.")
    add_argument(auth_group, "sk", required=False, help="BioOS secret key for bridge login.")
    add_argument(auth_group, "endpoint", required=False, default=None, help="BioOS endpoint for bridge login.")
    add_argument(auth_group, "region", required=False, default=None, help="BioOS region for bridge login.")
    add_argument(auth_group, "passport", required=False, help="Network passport token. Skips BioOS bridge login.")


def add_network_arguments(parser: argparse.ArgumentParser) -> None:
    group = parser.add_argument_group("Network")
    add_argument(group, "repository_endpoint", required=False, help="Network Repository endpoint.")


def add_output_arguments(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--output", choices=("json", "text"), default="json", help="Output format.")
    parser.add_argument("--pretty", action="store_true", help="Pretty-print structured output.")


def add_argument(parser: argparse.ArgumentParser, name: str, *args: Any, **kwargs: Any) -> None:
    cli_name = f"--{name.replace('_', '-')}"
    legacy_name = f"--{name}"
    names = [cli_name]
    if legacy_name != cli_name:
        names.append(legacy_name)
    parser.add_argument(*names, *args, dest=name, **kwargs)


def add_bool_argument(parser: argparse.ArgumentParser, name: str, default: bool, help_text: str) -> None:
    cli_name = f"--{name.replace('_', '-')}"
    legacy_name = f"--{name}"
    names = [cli_name]
    if legacy_name != cli_name:
        names.append(legacy_name)
    parser.add_argument(*names, dest=name, action="store_true", default=default, help=help_text)
    if default:
        negative_cli = f"--no-{name.replace('_', '-')}"
        negative_legacy = f"--no_{name}"
        negative_names = [negative_cli]
        if negative_legacy != negative_cli:
            negative_names.append(negative_legacy)
        parser.add_argument(*negative_names, dest=name, action="store_false", help=f"Disable: {help_text}")


def add_dataset_list_arguments(parser: argparse.ArgumentParser, library_scoped: bool = False) -> None:
    if library_scoped:
        add_argument(parser, "data_library_id", required=True, help="Data library ID.")
    else:
        add_argument(parser, "data_library_id", required=False, help="Data library ID.")
    add_argument(parser, "page", required=False, type=int, help="Page number.")
    add_argument(parser, "size", required=False, type=int, help="Page size.")
    add_argument(parser, "order_by", required=False, help="Order expression.")
    add_argument(parser, "search_word", required=False, help="Data set search word.")
    add_argument(parser, "id", required=False, action="append", help="Data set ID. Can be repeated.")
    add_argument(parser, "access_control", required=False, help="Data set access control.")
    add_argument(parser, "project_data_type", required=False, action="append", help="Project data type. Can be repeated.")
    add_argument(parser, "category", required=False, action="append", help="Category. Can be repeated.")
    add_argument(parser, "user_id", required=False, help="User ID.")
    add_bool_argument(parser, "mine", default=False, help_text="Use the current Network user as the user ID filter.")
    add_argument(parser, "catalogue", required=False, action="append", help="Catalogue. Can be repeated.")
    add_argument(parser, "display_level", required=False, help="Display level.")
    add_argument(parser, "group", required=False, help="Data access committee/group.")
    add_argument(parser, "data_file_id", required=False, help="Data file ID.")


def add_dataset_get_arguments(
    parser: argparse.ArgumentParser,
    library_scoped: bool = False,
    include_user_filter: bool = False,
) -> None:
    if library_scoped:
        add_argument(parser, "data_library_id", required=True, help="Data library ID.")
    else:
        add_argument(parser, "data_library_id", required=False, help="Data library ID.")
    add_argument(parser, "data_set_id", required=True, help="Data set ID.")
    add_argument(parser, "display_level", required=False, default="Full", help="Display level.")
    if include_user_filter:
        add_argument(parser, "user_id", required=False, help="User ID.")
        add_bool_argument(parser, "mine", default=False, help_text="Use the current Network user as the user ID filter.")


def add_dataset_files_arguments(parser: argparse.ArgumentParser, library_scoped: bool = False) -> None:
    add_dataset_get_arguments(parser, library_scoped=library_scoped)
    add_argument(parser, "page", required=False, type=int, help="Page number.")
    add_argument(parser, "size", required=False, type=int, help="Page size.")
    add_argument(parser, "order_by", required=False, help="Order expression.")
    add_argument(parser, "search_scope", required=False, action="append", help="Search scope. Can be repeated.")
    add_argument(parser, "search_word", required=False, help="Data file search word.")
    add_argument(parser, "time_search_scope", required=False, help="Time search scope.")
    add_argument(parser, "start_time", required=False, type=int, help="Time search start timestamp.")
    add_argument(parser, "end_time", required=False, type=int, help="Time search end timestamp.")
    add_argument(parser, "id", required=False, action="append", help="Data file ID. Can be repeated.")
    add_argument(parser, "file_type", required=False, action="append", help="File type. Can be repeated.")


def add_dataset_file_ids_arguments(parser: argparse.ArgumentParser, library_scoped: bool = False) -> None:
    add_dataset_files_arguments(parser, library_scoped=library_scoped)


def add_dataset_download_files_arguments(parser: argparse.ArgumentParser, library_scoped: bool = False) -> None:
    add_dataset_files_arguments(parser, library_scoped=library_scoped)
    add_argument(parser, "target", required=True, help="Local target directory.")
    add_argument(parser, "access_id", required=False, default="https", help="DRS access ID.")
    add_bool_argument(parser, "overwrite", default=False, help_text="Overwrite existing local files.")
    add_bool_argument(parser, "continue_on_error", default=False, help_text="Continue after a failed file download.")


def add_drs_arguments(parser: argparse.ArgumentParser, required: bool = True) -> None:
    add_argument(parser, "object_id", required=required, help="DRS object ID or drs:// URI.")


def add_drs_access_arguments(parser: argparse.ArgumentParser) -> None:
    add_drs_arguments(parser)
    add_argument(parser, "access_id", required=False, default="https", help="DRS access ID.")


def add_drs_download_arguments(parser: argparse.ArgumentParser) -> None:
    add_drs_access_arguments(parser)
    add_argument(parser, "target", required=False, default=".", help="Local target file path or directory.")
    add_bool_argument(parser, "overwrite", default=False, help_text="Overwrite an existing local file.")


def add_drs_locate_arguments(parser: argparse.ArgumentParser) -> None:
    add_argument(parser, "drs_path", required=True, help="DRS path to locate in the repository.")


def handle_library_list(args: argparse.Namespace):
    _login_with_args(args)
    library_resource = network.libraries(repository_endpoint=args.repository_endpoint)
    if args.mine:
        if args.id or args.display_name or args.organization_id:
            raise ValueError("--mine cannot be combined with --id, --display-name, or --organization-id.")
        libraries = library_resource.user(
            page=args.page,
            size=args.size,
            order_by=args.order_by,
        )
    else:
        libraries = library_resource.list(
            page=args.page,
            size=args.size,
            order_by=args.order_by,
            ids=args.id,
            display_name=args.display_name,
            organization_id=args.organization_id,
        )
    return records(libraries)


def handle_library_get(args: argparse.Namespace):
    _login_with_args(args)
    return network.libraries(repository_endpoint=args.repository_endpoint).get(args.data_library_id)


def handle_dataset_list(args: argparse.Namespace):
    _login_with_args(args)
    data_sets = network.datasets(repository_endpoint=args.repository_endpoint).list(
        page=args.page,
        size=args.size,
        order_by=args.order_by,
        search_word=args.search_word,
        data_library_id=args.data_library_id,
        ids=args.id,
        access_control=args.access_control,
        project_data_type=args.project_data_type,
        category=args.category,
        user_id=_dataset_user_id(args),
        catalogue=args.catalogue,
        display_level=args.display_level,
        group=args.group,
        data_file_id=args.data_file_id,
    )
    return records(data_sets)


def handle_library_dataset_list(args: argparse.Namespace):
    _login_with_args(args)
    data_sets = network.library(
        args.data_library_id,
        repository_endpoint=args.repository_endpoint,
    ).datasets.list(
        page=args.page,
        size=args.size,
        order_by=args.order_by,
        search_word=args.search_word,
        ids=args.id,
        access_control=args.access_control,
        project_data_type=args.project_data_type,
        category=args.category,
        user_id=_dataset_user_id(args),
        catalogue=args.catalogue,
        display_level=args.display_level,
        group=args.group,
        data_file_id=args.data_file_id,
    )
    return records(data_sets)


def handle_dataset_get(args: argparse.Namespace):
    _login_with_args(args)
    return network.dataset(
        args.data_set_id,
        data_library_id=args.data_library_id,
        repository_endpoint=args.repository_endpoint,
    ).get(display_level=args.display_level, user_id=_dataset_user_id(args))


def handle_library_dataset_get(args: argparse.Namespace):
    _login_with_args(args)
    return network.library(args.data_library_id, repository_endpoint=args.repository_endpoint).dataset(args.data_set_id).get(
        display_level=args.display_level,
        user_id=_dataset_user_id(args),
    )


def handle_dataset_files(args: argparse.Namespace):
    _login_with_args(args)
    files = network.dataset(
        args.data_set_id,
        data_library_id=args.data_library_id,
        repository_endpoint=args.repository_endpoint,
    ).files(**_file_filters(args))
    return records(files)


def handle_library_dataset_files(args: argparse.Namespace):
    _login_with_args(args)
    files = network.library(args.data_library_id, repository_endpoint=args.repository_endpoint).dataset(args.data_set_id).files(
        **_file_filters(args)
    )
    return records(files)


def handle_dataset_file_ids(args: argparse.Namespace):
    _login_with_args(args)
    ids = network.dataset(
        args.data_set_id,
        data_library_id=args.data_library_id,
        repository_endpoint=args.repository_endpoint,
    ).file_ids(**_id_filters(args))
    return {"ids": ids}


def handle_library_dataset_file_ids(args: argparse.Namespace):
    _login_with_args(args)
    ids = network.library(args.data_library_id, repository_endpoint=args.repository_endpoint).dataset(args.data_set_id).file_ids(
        **_id_filters(args)
    )
    return {"ids": ids}


def handle_dataset_download_files(args: argparse.Namespace):
    _login_with_args(args)
    return network.dataset(
        args.data_set_id,
        data_library_id=args.data_library_id,
        repository_endpoint=args.repository_endpoint,
    ).download_files(
        target=args.target,
        access_id=args.access_id,
        overwrite=args.overwrite,
        continue_on_error=args.continue_on_error,
        **_file_filters(args),
    )


def handle_library_dataset_download_files(args: argparse.Namespace):
    _login_with_args(args)
    return network.library(args.data_library_id, repository_endpoint=args.repository_endpoint).dataset(
        args.data_set_id
    ).download_files(
        target=args.target,
        access_id=args.access_id,
        overwrite=args.overwrite,
        continue_on_error=args.continue_on_error,
        **_file_filters(args),
    )


def handle_drs(args: argparse.Namespace):
    _login_with_args(args)
    return network.network(repository_endpoint=args.repository_endpoint).drs_object(args.object_id)


def handle_drs_access(args: argparse.Namespace):
    _login_with_args(args)
    return network.network(repository_endpoint=args.repository_endpoint).drs_access(args.object_id, access_id=args.access_id)


def handle_drs_download(args: argparse.Namespace):
    _login_with_args(args)
    return network.network(repository_endpoint=args.repository_endpoint).download_drs_object(
        args.object_id,
        target=args.target,
        access_id=args.access_id,
        overwrite=args.overwrite,
    )


def handle_drs_locate(args: argparse.Namespace):
    _login_with_args(args)
    return network.network(repository_endpoint=args.repository_endpoint).drs_locate(args.drs_path)


def _file_filters(args: argparse.Namespace) -> dict:
    return {
        "page": args.page,
        "size": args.size,
        "order_by": args.order_by,
        "search_scope": args.search_scope,
        "search_word": args.search_word,
        "time_search_scope": args.time_search_scope,
        "start_time": args.start_time,
        "end_time": args.end_time,
        "ids": args.id,
        "file_type": args.file_type,
    }


def _id_filters(args: argparse.Namespace) -> dict:
    filters = _file_filters(args)
    filters.pop("page", None)
    filters.pop("size", None)
    filters.pop("order_by", None)
    return filters


def _dataset_user_id(args: argparse.Namespace) -> Optional[str]:
    mine = getattr(args, "mine", False)
    user_id = getattr(args, "user_id", None)
    if mine and user_id:
        raise ValueError("--mine cannot be combined with --user-id.")
    if mine:
        return network.current_user_id()
    return user_id


def _login_with_args(args: argparse.Namespace) -> None:
    if getattr(args, "passport", None):
        network.login_with_passport(args.passport)
        return
    settings = resolve_auth_settings(
        access_key=getattr(args, "ak", None),
        secret_key=getattr(args, "sk", None),
        endpoint=getattr(args, "endpoint", None),
        region=getattr(args, "region", None),
    )
    ak, sk = resolve_credentials(getattr(args, "ak", None), getattr(args, "sk", None))
    network.login_with_bioos(
        access_key=ak,
        secret_key=sk,
        endpoint=settings["endpoint"] or DEFAULT_ENDPOINT,
        region=settings["region"] or "cn-north-1",
    )


def records(value: Any):
    if hasattr(value, "to_dict"):
        return value.to_dict(orient="records")
    return value


def run_cli(handler, args: argparse.Namespace) -> int:
    try:
        result = handler(args)
        emit_output(result, output=args.output, pretty=args.pretty)
        return 0
    except Exception as exc:
        emit_error(exc, output=args.output)
        return 1


def emit_output(data: Any, output: str = "json", pretty: bool = False) -> None:
    if output == "text" and isinstance(data, str):
        print(data)
        return
    print(json.dumps(data, ensure_ascii=False, indent=2 if pretty or output == "text" else None, default=_json_default))


def emit_error(exc: Exception, output: str = "json") -> None:
    if output == "text":
        print(str(exc), file=sys.stderr)
        return
    print(json.dumps({"success": False, "error": str(exc)}, ensure_ascii=False, default=_json_default), file=sys.stderr)


def _json_default(value: Any) -> Any:
    if hasattr(value, "to_pydatetime"):
        value = value.to_pydatetime()
    if hasattr(value, "item"):
        try:
            return value.item()
        except Exception:
            pass
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    if isinstance(value, Path):
        return str(value)
    if isinstance(value, set):
        return sorted(value)
    raise TypeError(f"Object of type {type(value).__name__} is not JSON serializable")


if __name__ == "__main__":
    sys.exit(main())
