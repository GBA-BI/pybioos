import subprocess
from pathlib import Path
from typing import Dict


def validate_wdl_file(wdl_path: str) -> Dict[str, object]:
    path = Path(wdl_path)
    if not path.exists():
        raise FileNotFoundError(f"WDL file not found: {wdl_path}")
    result = subprocess.run(
        ["womtool", "validate", str(path)],
        capture_output=True,
        text=True,
        check=True,
    )
    return {
        "success": True,
        "wdl_path": str(path),
        "stdout": result.stdout.strip(),
        "stderr": result.stderr.strip(),
    }


def validate_workflow_input_json_file(wdl_path: str, input_json: str) -> Dict[str, object]:
    wdl = Path(wdl_path)
    inputs = Path(input_json)
    if not wdl.exists():
        raise FileNotFoundError(f"WDL file not found: {wdl_path}")
    if not inputs.exists():
        raise FileNotFoundError(f"Input JSON not found: {input_json}")
    result = subprocess.run(
        ["womtool", "validate", str(wdl), "--inputs", str(inputs)],
        capture_output=True,
        text=True,
        check=True,
    )
    return {
        "success": True,
        "wdl_path": str(wdl),
        "input_json": str(inputs),
        "stdout": result.stdout.strip(),
        "stderr": result.stderr.strip(),
    }

