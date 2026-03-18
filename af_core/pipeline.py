from __future__ import annotations

import hashlib
import json
import os
import subprocess
import sys
from dataclasses import dataclass, asdict
from datetime import datetime, timezone
from enum import Enum
from pathlib import Path
from typing import Optional, Callable


class Mode(str, Enum):
    FETCH_QUERY = "fetch_query"       
    FETCH_DOI = "fetch_doi"          
    FILTER = "filter"      


@dataclass(frozen=True)
class PipelineConfig:
    mode: Mode
    
    runs_dir: str = "runs"
    use_cache: bool = True
    sleep: float = 0.25

    # Mode 1: Fetch Query settings
    usr_query: str | None = None
    database_id: str = "WOS"
    page_size: int = 100
    max_records: int | None = None
    start_date: str | None = None  
    end_date: str | None = None    

    # Mode 2: Fetch DOI settings
    doi_list_path: str | None = None

    # Mode 3: Filter settings
    input_wos_csv: str | None = None
    keywords_yml: str | None = None
    Contributor_csv: str | None = None
    do_Contributor_check: bool = True


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _hash_cfg(cfg: PipelineConfig) -> str:
    d = {k: v for k, v in asdict(cfg).items() if v is not None}
    payload = json.dumps(d, sort_keys=True).encode("utf-8")
    return hashlib.sha256(payload).hexdigest()[:12]


def _stream_cmd(cmd: list[str], cwd: Path, on_line: Optional[Callable[[str], None]] = None) -> None:
    p = subprocess.Popen(
        cmd,
        cwd=str(cwd),
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        bufsize=1,
    )
    
    output_lines = []
    assert p.stdout is not None
    
    for line in p.stdout:
        line = line.rstrip("\n")
        output_lines.append(line)
        if on_line:
            on_line(line)
            
    rc = p.wait()
    if rc != 0:
        error_context = "\n".join(output_lines[-10:]) if output_lines else "No output captured."
        raise RuntimeError(f"Command failed (code={rc}).\nLast output:\n{error_context}")


def run_pipeline(cfg: PipelineConfig, repo_root: Path, on_event: Optional[Callable[[dict], None]] = None) -> dict:
    repo_root = repo_root.resolve()
    run_id = _hash_cfg(cfg)
    run_dir = (repo_root / cfg.runs_dir / f"{cfg.mode.value}_{run_id}").resolve()
    run_dir.mkdir(parents=True, exist_ok=True)

    log_path = run_dir / "pipeline.log"
    manifest_path = run_dir / "manifest.json"

    def emit(event_type: str, **data):
        payload = {"type": event_type, "ts": _utc_now(), **data}
        if on_event:
            on_event(payload)
        with log_path.open("a", encoding="utf-8") as f:
            f.write(json.dumps(payload, ensure_ascii=False) + "\n")

    emit("start", mode=cfg.mode.value, run_dir=str(run_dir))

    fetched_csv_query = run_dir / "wos_results.csv"
    fetched_csv_doi = run_dir / "wos_results_by_doi.csv"
    filtered_csv = run_dir / "filtered_results.csv"
    Contributor_checked_csv = run_dir / "Contributor_names_checked.csv"

    artifacts = {
        "run_dir": str(run_dir),
        "log": str(log_path),
        "manifest": str(manifest_path),
    }

    
    # MODE 1: FETCH QUERY 
  
    if cfg.mode == Mode.FETCH_QUERY:
        if not cfg.usr_query:
            raise ValueError("FETCH_QUERY mode requires 'usr_query'.")

        emit("query_info", final_query=cfg.usr_query)

        if cfg.use_cache and fetched_csv_query.exists():
            emit("fetch_query_cache_hit", csv=str(fetched_csv_query))
        else:
            emit("fetch_query_start", usr_query=cfg.usr_query)
            cmd = [
                sys.executable,
                str(repo_root / "scripts" / "fetch_wos_query.py"),
                "--usr-query", cfg.usr_query,
                "--out-dir", str(run_dir),
                "--database-id", cfg.database_id,
                "--page-size", str(cfg.page_size),
                "--sleep", str(cfg.sleep),
                "--summary-csv", str(fetched_csv_query),
                
            ]
            
           
            if cfg.start_date:
                cmd += ["--start-date", cfg.start_date]
            if cfg.end_date:
                cmd += ["--end-date", cfg.end_date]
            if cfg.max_records is not None:
                cmd += ["--max-records", str(cfg.max_records)]
            
            _stream_cmd(cmd, cwd=repo_root, on_line=lambda s: emit("log", line=s))
            emit("fetch_query_done", csv=str(fetched_csv_query))
        
        artifacts["output_csv"] = str(fetched_csv_query)
       

    # MODE 2: FETCH DOI

    elif cfg.mode == Mode.FETCH_DOI:
        if not cfg.doi_list_path:
            raise ValueError("FETCH_DOI mode requires 'doi_list_path'.")

        if cfg.use_cache and fetched_csv_doi.exists():
            emit("fetch_doi_cache_hit", csv=str(fetched_csv_doi))
        else:
            emit("fetch_doi_start", doi_list_path=cfg.doi_list_path)
            cmd = [
                sys.executable,
                str(repo_root / "scripts" / "fetch_wos_by_doi.py"),
                "--doi-list", str(Path(cfg.doi_list_path).resolve()),
                "--out-dir", str(run_dir),
                "--out-csv", str(fetched_csv_doi),
                "--page-size", str(cfg.page_size),
                "--sleep", str(cfg.sleep),
            ]
            _stream_cmd(cmd, cwd=repo_root, on_line=lambda s: emit("log", line=s))
            emit("fetch_doi_done", csv=str(fetched_csv_doi))

        artifacts["output_csv"] = str(fetched_csv_doi)
        

  
    # MODE 3: FILTER 
    
    elif cfg.mode == Mode.FILTER:
        if not cfg.input_wos_csv:
            raise ValueError("FILTER mode requires 'input_wos_csv'.")
        if not cfg.keywords_yml:
            raise ValueError("FILTER mode requires 'keywords_yml'.")
        
        input_csv_path = Path(cfg.input_wos_csv).resolve()
        if not input_csv_path.exists():
             raise FileNotFoundError(f"Input file not found: {input_csv_path}")

        emit("filter_start", input_csv=str(input_csv_path))

        Contributor_csv_arg = cfg.Contributor_csv if (cfg.do_Contributor_check and cfg.Contributor_csv) else ""
        out_Contributor_checked_arg = str(Contributor_checked_csv) if (cfg.do_Contributor_check and cfg.Contributor_csv) else ""

        filter_cmd = [
            sys.executable,
            str(repo_root / "scripts" / "filter_wos_records.py"),
            "--wos", str(input_csv_path),
            "--keywords", str(Path(cfg.keywords_yml).resolve()),
            "--out", str(filtered_csv),
        ]
        if Contributor_csv_arg:
            filter_cmd += ["--Contributor", str(Path(Contributor_csv_arg).resolve())]
        if out_Contributor_checked_arg:
            filter_cmd += ["--out-Contributor-checked", out_Contributor_checked_arg]

        _stream_cmd(filter_cmd, cwd=repo_root, on_line=lambda s: emit("log", line=s))
        
        artifacts["filtered_csv"] = str(filtered_csv)
        if out_Contributor_checked_arg:
            artifacts["Contributor_checked_csv"] = str(Contributor_checked_csv)
        
        emit("filter_done", filtered_csv=str(filtered_csv))

    else:
        raise ValueError(f"Unknown mode: {cfg.mode}")

    manifest = {
        "run_id": run_id,
        "created_utc": _utc_now(),
        "config": asdict(cfg),
        "artifacts": artifacts,
    }
    manifest_path.write_text(json.dumps(manifest, indent=2), encoding="utf-8")
    emit("done", manifest=str(manifest_path))

    return manifest