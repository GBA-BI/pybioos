import argparse
import json
import sys
from datetime import date, datetime
from pathlib import Path
from typing import Any


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


def build_parser(description: str, include_auth: bool = True) -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=description)
    if include_auth:
        add_auth_arguments(parser)
    add_output_arguments(parser)
    return parser


def add_auth_arguments(parser: argparse.ArgumentParser) -> None:
    auth_group = parser.add_argument_group("Authentication overrides (advanced)")
    add_argument(
        auth_group,
        "ak",
        required=False,
        help="Bio-OS access key. Falls back to environment variables or ~/.bioos/config.yaml.",
    )
    add_argument(
        auth_group,
        "sk",
        required=False,
        help="Bio-OS secret key. Falls back to environment variables or ~/.bioos/config.yaml.",
    )
    add_argument(
        auth_group,
        "endpoint",
        required=False,
        default=None,
        help="Bio-OS endpoint. Falls back to BIOOS_ENDPOINT, ~/.bioos/config.yaml, or the SDK default.",
    )


def add_output_arguments(parser: argparse.ArgumentParser) -> None:
    parser.add_argument(
        "--output",
        choices=("json", "text"),
        default="json",
        help="Output format. Defaults to json.",
    )
    parser.add_argument(
        "--pretty",
        action="store_true",
        help="Pretty-print structured output.",
    )


def add_argument(parser: argparse.ArgumentParser, name: str, *args: Any, **kwargs: Any) -> None:
    cli_name = f"--{name.replace('_', '-')}"
    legacy_name = f"--{name}"
    option_names = [cli_name]
    if legacy_name != cli_name:
        option_names.append(legacy_name)
    parser.add_argument(*option_names, *args, dest=name, **kwargs)


def add_bool_argument(
    parser: argparse.ArgumentParser,
    name: str,
    default: bool,
    help_text: str,
) -> None:
    cli_name = f"--{name.replace('_', '-')}"
    legacy_name = f"--{name}"
    option_names = [cli_name]
    if legacy_name != cli_name:
        option_names.append(legacy_name)
    parser.add_argument(
        *option_names,
        dest=name,
        action="store_true",
        default=default,
        help=help_text,
    )
    if default:
        negative_cli = f"--no-{name.replace('_', '-')}"
        negative_legacy = f"--no_{name}"
        negative_option_names = [negative_cli]
        if negative_legacy != negative_cli:
            negative_option_names.append(negative_legacy)
        parser.add_argument(
            *negative_option_names,
            dest=name,
            action="store_false",
            help=f"Disable: {help_text}",
        )


def emit_output(data: Any, output: str = "json", pretty: bool = False) -> None:
    if output == "text" and isinstance(data, str):
        print(data)
        return

    indent = 2 if pretty or output == "text" else None
    print(json.dumps(data, ensure_ascii=False, indent=indent, default=_json_default))


def emit_error(exc: Exception, output: str = "json") -> None:
    if output == "text":
        print(str(exc), file=sys.stderr)
        return
    print(
        json.dumps({"success": False, "error": str(exc)}, ensure_ascii=False, default=_json_default),
        file=sys.stderr,
    )


def run_cli(handler, args: argparse.Namespace) -> int:
    try:
        result = handler(args)
        emit_output(result, output=args.output, pretty=args.pretty)
        return 0
    except Exception as exc:
        emit_error(exc, output=args.output)
        return 1
