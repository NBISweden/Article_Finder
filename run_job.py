from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--repo-root", required=True)
    ap.add_argument("--config", required=True)
    args = ap.parse_args()

    repo_root = Path(args.repo_root).resolve()
    cfg_path = Path(args.config).resolve()

    if str(repo_root) not in sys.path:
        sys.path.insert(0, str(repo_root))

    try:
        from af_core.pipeline import Mode, PipelineConfig, run_pipeline
    except ImportError:
        from pipeline import Mode, PipelineConfig, run_pipeline

    payload = json.loads(cfg_path.read_text(encoding="utf-8"))
    payload["mode"] = Mode(payload["mode"])

    cfg = PipelineConfig(**payload)
    run_pipeline(cfg, repo_root=repo_root)


if __name__ == "__main__":
    main()