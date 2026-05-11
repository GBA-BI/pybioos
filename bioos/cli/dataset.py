import sys

from bioos.cli.common import add_argument, add_bool_argument, build_parser, run_cli
from bioos.ops.auth import login_with_args
from bioos.ops.formatters import dataframe_records


def build_list_args():
    parser = build_parser("List BioOS Network data sets.")
    add_dataset_list_arguments(parser)
    return parser


def build_files_args():
    parser = build_parser("List files under a BioOS Network data set.")
    add_dataset_files_arguments(parser)
    return parser


def build_get_args():
    parser = build_parser("Get a BioOS Network data set.")
    add_dataset_get_arguments(parser)
    return parser


def build_file_ids_args():
    parser = build_parser("List file IDs under a BioOS Network data set.")
    add_dataset_file_ids_arguments(parser)
    return parser


def build_download_files_args():
    parser = build_parser("Download files under a BioOS Network data set.")
    add_dataset_download_files_arguments(parser)
    return parser


def build_drs_args():
    parser = build_parser("Get a GA4GH DRS object.")
    add_drs_arguments(parser)
    return parser


def build_drs_access_args():
    parser = build_parser("Get a GA4GH DRS object access URL.")
    add_drs_access_arguments(parser)
    return parser


def build_drs_download_args():
    parser = build_parser("Download a GA4GH DRS object.")
    add_drs_download_arguments(parser)
    return parser


def add_dataset_list_arguments(parser):
    add_argument(parser, "page", required=False, type=int, help="Page number.")
    add_argument(parser, "size", required=False, type=int, help="Page size.")
    add_argument(parser, "order_by", required=False, help="Order expression, e.g. createTime:desc,name:asc.")
    add_argument(parser, "search_word", required=False, help="Data set name search word.")
    add_argument(parser, "data_library_id", required=False, help="Data library ID.")
    add_argument(parser, "id", required=False, action="append", help="Data set ID. Can be specified multiple times.")
    add_argument(parser, "access_control", required=False, help="Data set access control.")
    add_argument(
        parser,
        "project_data_type",
        required=False,
        action="append",
        help="Project data type. Can be specified multiple times.",
    )
    add_argument(parser, "category", required=False, action="append", help="Category. Can be specified multiple times.")
    add_argument(parser, "user_id", required=False, help="User ID.")
    add_argument(parser, "catalogue", required=False, action="append", help="Catalogue. Can be specified multiple times.")
    add_argument(parser, "display_level", required=False, help="Display level: Minimal or Full.")
    add_argument(parser, "group", required=False, help="Data access committee/group.")
    add_argument(parser, "data_file_id", required=False, help="Data file ID.")


def add_dataset_get_arguments(parser):
    add_argument(parser, "data_set_id", required=True, help="Data set ID.")
    add_argument(parser, "data_library_id", required=False, help="Data library ID.")
    add_argument(parser, "display_level", required=False, default="Full", help="Display level: Minimal or Full.")


def add_dataset_files_arguments(parser):
    add_argument(parser, "data_set_id", required=True, help="Data set ID.")
    add_argument(parser, "data_library_id", required=True, help="Data library ID.")
    add_argument(parser, "page", required=False, type=int, help="Page number.")
    add_argument(parser, "size", required=False, type=int, help="Page size.")
    add_argument(parser, "order_by", required=False, help="Order expression, e.g. createTime:desc,name:asc.")
    add_argument(
        parser,
        "search_scope",
        required=False,
        action="append",
        help="Search scope. Can be specified multiple times.",
    )
    add_argument(parser, "search_word", required=False, help="Data file search word.")
    add_argument(parser, "time_search_scope", required=False, help="Time search scope: create_time or update_time.")
    add_argument(parser, "start_time", required=False, type=int, help="Time search start timestamp.")
    add_argument(parser, "end_time", required=False, type=int, help="Time search end timestamp.")
    add_argument(parser, "id", required=False, action="append", help="Data file ID. Can be specified multiple times.")
    add_argument(parser, "file_type", required=False, action="append", help="File type. Can be specified multiple times.")


def add_dataset_file_ids_arguments(parser):
    add_argument(parser, "data_set_id", required=True, help="Data set ID.")
    add_argument(parser, "data_library_id", required=True, help="Data library ID.")
    add_argument(
        parser,
        "search_scope",
        required=False,
        action="append",
        help="Search scope. Can be specified multiple times.",
    )
    add_argument(parser, "search_word", required=False, help="Data file search word.")
    add_argument(parser, "time_search_scope", required=False, help="Time search scope: create_time or update_time.")
    add_argument(parser, "start_time", required=False, type=int, help="Time search start timestamp.")
    add_argument(parser, "end_time", required=False, type=int, help="Time search end timestamp.")
    add_argument(parser, "id", required=False, action="append", help="Data file ID. Can be specified multiple times.")
    add_argument(parser, "file_type", required=False, action="append", help="File type. Can be specified multiple times.")


def add_dataset_download_files_arguments(parser):
    add_dataset_files_arguments(parser)
    add_argument(parser, "target", required=True, help="Local target directory.")
    add_argument(parser, "access_id", required=False, default="https", help="DRS access ID. Defaults to https.")
    add_bool_argument(parser, "overwrite", default=False, help_text="Overwrite existing local files.")
    add_bool_argument(parser, "continue_on_error", default=False, help_text="Continue downloading remaining files after a failure.")


def add_drs_arguments(parser, required=True):
    add_argument(parser, "object_id", required=required, help="DRS object ID or drs:// URI.")


def add_drs_access_arguments(parser):
    add_drs_arguments(parser)
    add_argument(parser, "access_id", required=False, default="https", help="DRS access ID. Defaults to https.")


def add_drs_download_arguments(parser):
    add_drs_access_arguments(parser)
    add_argument(parser, "target", required=False, default=".", help="Local target file path or directory.")
    add_bool_argument(parser, "overwrite", default=False, help_text="Overwrite an existing local file.")


def handle_list(args):
    from bioos import bioos

    login_with_args(args)
    data_sets = bioos.network().datasets.list(
        page=args.page,
        size=args.size,
        order_by=args.order_by,
        search_word=args.search_word,
        data_library_id=args.data_library_id,
        ids=args.id,
        access_control=args.access_control,
        project_data_type=args.project_data_type,
        category=args.category,
        user_id=args.user_id,
        catalogue=args.catalogue,
        display_level=args.display_level,
        group=args.group,
        data_file_id=args.data_file_id,
    )
    return dataframe_records(data_sets)


def handle_get(args):
    from bioos import bioos

    login_with_args(args)
    return bioos.network().dataset(args.data_set_id, data_library_id=args.data_library_id).get(
        display_level=args.display_level,
    )


def handle_files(args):
    from bioos import bioos

    login_with_args(args)
    files = bioos.network().dataset(args.data_set_id, data_library_id=args.data_library_id).files(
        page=args.page,
        size=args.size,
        order_by=args.order_by,
        search_scope=args.search_scope,
        search_word=args.search_word,
        time_search_scope=args.time_search_scope,
        start_time=args.start_time,
        end_time=args.end_time,
        ids=args.id,
        file_type=args.file_type,
    )
    return dataframe_records(files)


def handle_file_ids(args):
    from bioos import bioos

    login_with_args(args)
    ids = bioos.network().dataset(args.data_set_id, data_library_id=args.data_library_id).file_ids(
        search_scope=args.search_scope,
        search_word=args.search_word,
        time_search_scope=args.time_search_scope,
        start_time=args.start_time,
        end_time=args.end_time,
        ids=args.id,
        file_type=args.file_type,
    )
    return {"ids": ids}


def handle_download_files(args):
    from bioos import bioos

    login_with_args(args)
    return bioos.network().dataset(args.data_set_id, data_library_id=args.data_library_id).download_files(
        target=args.target,
        access_id=args.access_id,
        overwrite=args.overwrite,
        continue_on_error=args.continue_on_error,
        page=args.page,
        size=args.size,
        order_by=args.order_by,
        search_scope=args.search_scope,
        search_word=args.search_word,
        time_search_scope=args.time_search_scope,
        start_time=args.start_time,
        end_time=args.end_time,
        ids=args.id,
        file_type=args.file_type,
    )


def handle_drs(args):
    from bioos import bioos

    login_with_args(args)
    return bioos.network().drs_object(args.object_id)


def handle_drs_access(args):
    from bioos import bioos

    login_with_args(args)
    return bioos.network().drs_access(args.object_id, access_id=args.access_id)


def handle_drs_download(args):
    from bioos import bioos

    login_with_args(args)
    return bioos.network().download_drs_object(
        args.object_id,
        target=args.target,
        access_id=args.access_id,
        overwrite=args.overwrite,
    )


def main_list():
    parser = build_list_args()
    args = parser.parse_args()
    sys.exit(run_cli(handle_list, args))


def main_get():
    parser = build_get_args()
    args = parser.parse_args()
    sys.exit(run_cli(handle_get, args))


def main_files():
    parser = build_files_args()
    args = parser.parse_args()
    sys.exit(run_cli(handle_files, args))


def main_file_ids():
    parser = build_file_ids_args()
    args = parser.parse_args()
    sys.exit(run_cli(handle_file_ids, args))


def main_download_files():
    parser = build_download_files_args()
    args = parser.parse_args()
    sys.exit(run_cli(handle_download_files, args))


def main_drs():
    parser = build_drs_args()
    args = parser.parse_args()
    sys.exit(run_cli(handle_drs, args))


def main_drs_access():
    parser = build_drs_access_args()
    args = parser.parse_args()
    sys.exit(run_cli(handle_drs_access, args))


def main_drs_download():
    parser = build_drs_download_args()
    args = parser.parse_args()
    sys.exit(run_cli(handle_drs_download, args))


if __name__ == "__main__":
    main_list()
