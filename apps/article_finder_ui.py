import marimo

__generated_with = "0.19.2"
app = marimo.App(css_file="custom.css")


@app.cell
def imports():
    import marimo as mo
    from pathlib import Path
    import sys
    import os
    import json
    import signal
    import subprocess
    import time
    from dataclasses import asdict
    import pandas as pd

    return Path, asdict, json, mo, os, pd, signal, subprocess, sys, time


@app.cell
def setup_pipeline(Path, sys):
    repo_root = Path(".").resolve()
    if str(repo_root) not in sys.path:
        sys.path.insert(0, str(repo_root))

    try:
        from af_core.pipeline import Mode, PipelineConfig, _hash_cfg
    except ImportError:
        try:
            from pipeline import Mode, PipelineConfig, _hash_cfg
        except ImportError:
            raise ImportError("Could not import pipeline.py. Ensure it exists.")

    return Mode, PipelineConfig, _hash_cfg, repo_root


@app.cell
def ui_state(mo):
    advanced_open, set_advanced_open = mo.state(False, allow_self_loops=True)
    last_manifest, set_last_manifest = mo.state(None, allow_self_loops=True)

    runner_pid, set_runner_pid = mo.state(None, allow_self_loops=True)
    current_manifest_path, set_current_manifest_path = mo.state("", allow_self_loops=True)
    current_log_path, set_current_log_path = mo.state("", allow_self_loops=True)
    is_running, set_is_running = mo.state(False, allow_self_loops=True)

    return (
        advanced_open,
        set_advanced_open,
        last_manifest,
        set_last_manifest,
        runner_pid,
        set_runner_pid,
        current_manifest_path,
        set_current_manifest_path,
        current_log_path,
        set_current_log_path,
        is_running,
        set_is_running,
    )


@app.cell
def tab_state(mo):
    selected_tab, set_selected_tab = mo.state("Fetch")
    return selected_tab, set_selected_tab


@app.cell
def helpers(os, subprocess):
    def get_widget_content(widget_object):
        files = widget_object.value
        if files and len(files) > 0:
            f = files[0]
            if hasattr(f, "name") and hasattr(f, "contents"):
                return f.name, f.contents
        return None, None

    def get_widget_files(widget_object):
        files = widget_object.value
        out = []

        if not files:
            return out

        for f in files:
            if hasattr(f, "name") and hasattr(f, "contents"):
                out.append((f.name, f.contents))

        return out

    def pid_is_running(pid):
        if pid is None:
            return False

        try:
            if os.name == "nt":
                res = subprocess.run(
                    ["tasklist", "/FI", f"PID eq {pid}"],
                    capture_output=True,
                    text=True,
                    check=False,
                )
                return str(pid) in res.stdout
            else:
                os.kill(pid, 0)
                return True
        except Exception:
            return False

    return get_widget_content, get_widget_files, pid_is_running

@app.cell
def advanced_controls(advanced_open, mo, set_advanced_open):
    advanced_btn = mo.ui.button(
        label=f"{'➖' if advanced_open() else '➕'} Advanced",
        kind="neutral",
        on_click=lambda _: set_advanced_open(not advanced_open()),
    )
    return (advanced_btn,)


@app.cell
def ui_elements(mo):
    FETCH_LABEL_TO_CODE = {
        "Fetch by WoS query": "fetch_query",
        "Fetch by DOI list": "fetch_doi",
    }

    MAX_UPLOAD = 500_000_000

    use_cache = mo.ui.checkbox(label="Use cache", value=True)

    fetch_mode = mo.ui.dropdown(
        options=list(FETCH_LABEL_TO_CODE.keys()),
        value="Fetch by WoS query",
        label="Fetch mode",
    )

    start_day_month_year = mo.ui.text(
        label="Start Date (DD-MM-YYYY)",
        value="01-01-2025",
    )
    end_day_month_year = mo.ui.text(
        label="End Date (DD-MM-YYYY)",
        value="31-12-2025",
    )
    usr_query = mo.ui.text(label="WoS usrQuery", value="CU=(Sweden)")
    max_records = mo.ui.number(label="Max records (0 = no limit)", value=1)

    doi_upload = mo.ui.file(
        kind="button",
        filetypes=[".txt", ".csv", ".tsv", ".xlsx", ".xls"],
        label="DOI List",
        max_size=MAX_UPLOAD,
    )
    doi_help_btn = mo.ui.button(
        label=f"{mo.icon('lucide:help-circle')}",
        kind="neutral",
        tooltip="TXT or CSV/XLSX with DOI column",
    )
    doi_header = mo.hstack(
        [mo.md("**Upload DOI list**"), doi_help_btn],
        justify="start",
        gap=0.25,
    )

    wos_csv_upload = mo.ui.file(
        kind="button",
        filetypes=[".csv", ".tsv", ".xlsx", ".xls"],
        label="WoS Results",
        max_size=MAX_UPLOAD,
    )
    keywords_upload = mo.ui.file(
        kind="button",
        filetypes=[".yml", ".yaml"],
        label="Keywords (yaml)",
        max_size=20_000_000,
    )
    keywords_help_btn = mo.ui.button(
        label=f"{mo.icon('lucide:help-circle')}",
        kind="neutral",
        tooltip="YAML with include/exclude terms",
    )
    keywords_header = mo.hstack(
        [mo.md("**Keywords**"), keywords_help_btn],
        justify="start",
        gap=0.25,
    )

    do_keyword_filter = mo.ui.checkbox(value=True)
    do_keyword_filter_row = mo.hstack(
        [do_keyword_filter, mo.plain_text("Run keyword search")],
        justify="start",
        gap=0.5,
    )

    do_Contributor_check = mo.ui.checkbox(value=False)
    do_Contributor_check_row = mo.hstack(
        [do_Contributor_check, mo.plain_text("Run name check (optional)")],
        justify="start",
        gap=0.5,
    )

    do_merge_results = mo.ui.checkbox(value=False)
    do_merge_results_row = mo.hstack(
        [do_merge_results, mo.plain_text("Create merged result file")],
        justify="start",
        gap=0.5,
    )

    Contributor_upload = mo.ui.file(
        kind="button",
        filetypes=[".csv"],
        label="Name List (csv)",
        max_size=MAX_UPLOAD,
    )
    Contributor_header = mo.hstack(
        [mo.md("**PI and Staff name check**")],
        justify="start",
    )

    page_size = mo.ui.number(label="Page size", value=100)
    sleep = mo.ui.number(label="Sleep between calls (seconds)", value=0.25)

    fetch_run_btn = mo.ui.run_button(label="Run Fetch", kind="success")
    filter_run_btn = mo.ui.run_button(label="Run Filter", kind="success")
    stop_run_btn = mo.ui.run_button(label="Stop Run", kind="danger")
    auto_refresh = mo.ui.refresh(default_interval="5s", label="")
    compare_wos_upload = mo.ui.file(
        kind="button",
        filetypes=[".csv", ".tsv", ".xlsx", ".xls"],
        label="Filtered / merged WoS result",
        max_size=MAX_UPLOAD,
    )

    manual_compare_upload = mo.ui.file(
        kind="button",
        filetypes=[".csv", ".tsv", ".xlsx", ".xls"],
        label="Manual publication file(s)",
        max_size=MAX_UPLOAD,
        multiple=True,
    )

    compare_year_window = mo.ui.number(
        label="Accept WoS year up to N years after manual year",
        value=1,
    )

    compare_run_btn = mo.ui.run_button(
        label="Run Compare",
        kind="success",
    )
    return (
        Contributor_header,
        Contributor_upload,
        FETCH_LABEL_TO_CODE,
        do_keyword_filter,
        do_keyword_filter_row,
        do_Contributor_check,
        do_Contributor_check_row,
        do_merge_results,
        do_merge_results_row,
        doi_header,
        doi_upload,
        end_day_month_year,
        fetch_mode,
        fetch_run_btn,
        filter_run_btn,
        stop_run_btn,
        auto_refresh,
        keywords_header,
        keywords_upload,
        max_records,
        page_size,
        sleep,
        start_day_month_year,
        use_cache,
        usr_query,
        wos_csv_upload,
        compare_wos_upload,
        manual_compare_upload,
        compare_year_window,
        compare_run_btn,
    )


@app.cell
def main_layout(
    Contributor_header,
    Contributor_upload,
    advanced_btn,
    advanced_open,
    auto_refresh,
    is_running,
    do_keyword_filter_row,
    do_Contributor_check_row,
    do_merge_results_row,
    doi_header,
    doi_upload,
    selected_tab,
    set_selected_tab,
    end_day_month_year,
    fetch_mode,
    fetch_run_btn,
    filter_run_btn,
    stop_run_btn,
    keywords_header,
    keywords_upload,
    max_records,
    mo,
    page_size,
    sleep,
    start_day_month_year,
    use_cache,
    usr_query,
    wos_csv_upload,
    compare_wos_upload,
    manual_compare_upload,
    compare_year_window,
    compare_run_btn,
):
    _advanced_panel = (
        mo.vstack(
            [
                mo.md("**Advanced settings**"),
                page_size,
                sleep,
            ],
            gap=0.5,
        )
        if advanced_open()
        else mo.md("")
    )

    _fetch_query_panel = mo.vstack(
        [
            usr_query,
            start_day_month_year,
            end_day_month_year,
            max_records,
            advanced_btn,
            _advanced_panel,
            mo.md("---"),
            mo.hstack([fetch_run_btn, stop_run_btn], justify="end", gap=0.5),
        ],
        gap=0.5,
    )

    _fetch_doi_panel = mo.vstack(
        [
            doi_header,
            doi_upload,
            advanced_btn,
            _advanced_panel,
            mo.md("---"),
            mo.hstack([fetch_run_btn, stop_run_btn], justify="end", gap=0.5),
        ],
        gap=0.5,
    )

    _current_fetch_sub_panel = (
        _fetch_query_panel
        if fetch_mode.value == "Fetch by WoS query"
        else _fetch_doi_panel
    )

    _fetch_panel = mo.vstack(
        [
            fetch_mode,
            mo.md("---"),
            _current_fetch_sub_panel,
        ],
        gap=0.75,
    )

    _filter_panel = mo.vstack(
        [
            mo.md("**Upload a WoS file to run filtering.**"),
            wos_csv_upload,
            do_keyword_filter_row,
            keywords_header,
            keywords_upload,
            Contributor_header,
            do_Contributor_check_row,
            Contributor_upload,
            do_merge_results_row,
            mo.md("---"),
            mo.hstack([filter_run_btn, stop_run_btn], justify="end", gap=0.5),
        ],
        gap=0.5,
    )

    _compare_panel = mo.vstack(
        [
            mo.md("**Compare manually collected publication files against filtered WoS results.**"),
            compare_wos_upload,
            manual_compare_upload,
            compare_year_window,
            mo.md("---"),
            mo.hstack([compare_run_btn, stop_run_btn], justify="end", gap=0.5),
        ],
        gap=0.5,
    )

    _main_tabs = mo.ui.tabs(
        {
            "Fetch": _fetch_panel,
            "Filter": _filter_panel,
            "Compare": _compare_panel,
        },
        value=selected_tab(),
        on_change=set_selected_tab,
    )
    
    _hidden_refresh = (
    mo.md(f"<div style='display:none'>{auto_refresh}</div>")
    if is_running()
    else mo.md("")
    )

    _cache_control = (
        mo.vstack(
            [
                mo.md("---"),
                use_cache,
            ]
        )
        if selected_tab() in ["Fetch", "Filter"]
        else mo.md("")
    )

    app_ui = mo.vstack(
        [
            mo.center(mo.md("# Article Finder")),
            _main_tabs,
            _cache_control,
            _hidden_refresh,
        ]
    )
    return app_ui


@app.cell
def display_ui(app_ui):
    app_ui
    return


@app.cell
def launcher(
    Path,
    _hash_cfg,
    asdict,
    current_log_path,
    current_manifest_path,
    json,
    os,
    repo_root,
    set_current_log_path,
    set_current_manifest_path,
    set_is_running,
    set_last_manifest,
    set_runner_pid,
    subprocess,
    sys,
):
    def launch_config(cfg):
        upload_dir = repo_root / "runs" / "_uploads"
        upload_dir.mkdir(parents=True, exist_ok=True)

        run_id = _hash_cfg(cfg)
        run_dir = (repo_root / cfg.runs_dir / f"{cfg.mode.value}_{run_id}").resolve()
        manifest_path = run_dir / "manifest.json"
        log_path = run_dir / "pipeline.log"

        cfg_path = upload_dir / f"job_{cfg.mode.value}_{run_id}.json"
        payload = asdict(cfg)
        payload["mode"] = cfg.mode.value
        cfg_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")

        cmd = [
            sys.executable,
            str(repo_root / "run_job.py"),
            "--repo-root", str(repo_root),
            "--config", str(cfg_path),
        ]

        kwargs = {"cwd": str(repo_root)}
        if os.name == "nt":
            kwargs["creationflags"] = getattr(subprocess, "CREATE_NEW_PROCESS_GROUP", 0)
        else:
            kwargs["start_new_session"] = True

        proc = subprocess.Popen(cmd, **kwargs)

        set_runner_pid(proc.pid)
        set_current_manifest_path(str(manifest_path))
        set_current_log_path(str(log_path))
        set_last_manifest(None)
        set_is_running(True)

        return proc.pid, manifest_path, log_path

    return (launch_config,)


@app.cell
def check_run_status(
    Path,
    auto_refresh,
    current_manifest_path,
    is_running,
    json,
    last_manifest,
    pid_is_running,
    runner_pid,
    set_is_running,
    set_last_manifest,
    set_runner_pid,
):
    _ = auto_refresh.value

    _manifest_path = current_manifest_path()
    if _manifest_path and Path(_manifest_path).exists():
        try:
            _manifest = json.loads(Path(_manifest_path).read_text(encoding="utf-8"))
            set_last_manifest(_manifest)
        except Exception:
            pass

    _pid = runner_pid()
    if is_running() and _pid is not None and not pid_is_running(_pid):
        set_is_running(False)
        set_runner_pid(None)

    return


@app.cell
def run_fetch_logic(
    FETCH_LABEL_TO_CODE,
    Mode,
    Path,
    PipelineConfig,
    doi_upload,
    end_day_month_year,
    fetch_mode,
    fetch_run_btn,
    get_widget_content,
    is_running,
    launch_config,
    max_records,
    mo,
    page_size,
    pid_is_running,
    runner_pid,
    set_is_running,
    set_runner_pid,
    sleep,
    start_day_month_year,
    use_cache,
    usr_query,
):
    mo.stop(not fetch_run_btn.value)

    _existing_pid = runner_pid()
    if is_running():
        if _existing_pid is not None and pid_is_running(_existing_pid):
            mo.stop(True)
        else:
            set_is_running(False)
            set_runner_pid(None)

    _fetch_label = fetch_mode.value
    _mode_code = FETCH_LABEL_TO_CODE.get(_fetch_label, "fetch_query")
    _current_run_mode = Mode(_mode_code)

    _upload_dir = Path("runs") / "_uploads"
    _upload_dir.mkdir(parents=True, exist_ok=True)

    _doi_path = None
    _s_date, _e_date = None, None

    if _current_run_mode == Mode.FETCH_QUERY:
        try:
            _s_p = start_day_month_year.value.split("-")
            _e_p = end_day_month_year.value.split("-")
            _s_date = f"{_s_p[2]}-{_s_p[1]}-{_s_p[0]}"
            _e_date = f"{_e_p[2]}-{_e_p[1]}-{_e_p[0]}"
        except Exception:
            raise ValueError("Use DD-MM-YYYY format.")

    elif _current_run_mode == Mode.FETCH_DOI:
        _d_n, _d_c = get_widget_content(doi_upload)
        if not _d_n:
            raise ValueError("Upload a DOI file.")

        _p = _upload_dir / f"doi_{Path(_d_n).name}"
        _p.write_bytes(_d_c)
        _doi_path = str(_p)

    _cfg = PipelineConfig(
        mode=_current_run_mode,
        use_cache=bool(use_cache.value),
        page_size=int(page_size.value),
        sleep=float(sleep.value),
        usr_query=str(usr_query.value).strip()
        if _current_run_mode == Mode.FETCH_QUERY
        else None,
        max_records=int(max_records.value)
        if int(max_records.value) != 0
        else None,
        start_date=_s_date,
        end_date=_e_date,
        doi_list_path=_doi_path,
        input_wos_csv=None,
        keywords_yml=None,
        do_keyword_filter=True,
        do_Contributor_check=False,
        do_merge_results=False,
        Contributor_csv=None,
    )

    _fetch_pid, _fetch_manifest_path, _fetch_log_path = launch_config(_cfg)
    print(f"Started {_current_run_mode.value} with PID {_fetch_pid}")
    print(f"Manifest path: {_fetch_manifest_path}")
    print(f"Log path: {_fetch_log_path}")


@app.cell
def run_filter_logic(
    Contributor_upload,
    Mode,
    Path,
    PipelineConfig,
    do_keyword_filter,
    do_Contributor_check,
    do_merge_results,
    filter_run_btn,
    get_widget_content,
    is_running,
    keywords_upload,
    launch_config,
    mo,
    page_size,
    pid_is_running,
    runner_pid,
    set_is_running,
    set_runner_pid,
    sleep,
    use_cache,
    wos_csv_upload,
):
    mo.stop(not filter_run_btn.value)

    _existing_pid = runner_pid()
    if is_running():
        if _existing_pid is not None and pid_is_running(_existing_pid):
            mo.stop(True)
        else:
            set_is_running(False)
            set_runner_pid(None)

    _current_run_mode = Mode("filter")

    _upload_dir = Path("runs") / "_uploads"
    _upload_dir.mkdir(parents=True, exist_ok=True)

    _w_n, _w_c = get_widget_content(wos_csv_upload)
    if not _w_n:
        raise ValueError("Upload WoS CSV.")

    _p_w = _upload_dir / f"wos_{Path(_w_n).name}"
    _p_w.write_bytes(_w_c)
    _wos_path = str(_p_w)

    _kw_path = None
    _k_n, _k_c = get_widget_content(keywords_upload)
    if _k_n:
        _p_k = _upload_dir / f"keywords_{Path(_k_n).name}"
        _p_k.write_bytes(_k_c)
        _kw_path = str(_p_k)

    _eff_k_check = bool(do_keyword_filter.value)
    if _eff_k_check and not _kw_path:
        raise ValueError("Upload Keywords YAML.")

    _c_path = None
    _eff_c_check = False
    if do_Contributor_check.value:
        _cn, _cc = get_widget_content(Contributor_upload)
        if not _cn:
            raise ValueError("Upload Contributor CSV.")

        _p_c = _upload_dir / f"Contributor_{Path(_cn).name}"
        _p_c.write_bytes(_cc)
        _c_path = str(_p_c)
        _eff_c_check = True

    _cfg = PipelineConfig(
        mode=_current_run_mode,
        use_cache=bool(use_cache.value),
        page_size=int(page_size.value),
        sleep=float(sleep.value),
        usr_query=None,
        max_records=None,
        start_date=None,
        end_date=None,
        doi_list_path=None,
        input_wos_csv=_wos_path,
        keywords_yml=_kw_path,
        do_keyword_filter=_eff_k_check,
        do_Contributor_check=_eff_c_check,
        do_merge_results=bool(do_merge_results.value),
        Contributor_csv=_c_path,
    )

    _filter_pid, _filter_manifest_path, _filter_log_path = launch_config(_cfg)
    print(f"Started {_current_run_mode.value} with PID {_filter_pid}")
    print(f"Manifest path: {_filter_manifest_path}")
    print(f"Log path: {_filter_log_path}")

@app.cell
def run_compare_logic(
    Mode,
    Path,
    PipelineConfig,
    compare_run_btn,
    compare_wos_upload,
    compare_year_window,
    get_widget_content,
    get_widget_files,
    is_running,
    launch_config,
    manual_compare_upload,
    mo,
    page_size,
    pid_is_running,
    runner_pid,
    set_is_running,
    set_runner_pid,
    sleep,
    time,
):
    mo.stop(not compare_run_btn.value)

    _existing_pid = runner_pid()
    if is_running():
        if _existing_pid is not None and pid_is_running(_existing_pid):
            mo.stop(True)
        else:
            set_is_running(False)
            set_runner_pid(None)

    _current_run_mode = Mode("compare")

    _upload_dir = Path("runs") / "_uploads"
    _upload_dir.mkdir(parents=True, exist_ok=True)

    _w_n, _w_c = get_widget_content(compare_wos_upload)
    if not _w_n:
        raise ValueError("Upload a filtered WoS file or merged result file.")

    _p_w = _upload_dir / f"compare_wos_{Path(_w_n).name}"
    _p_w.write_bytes(_w_c)
    _compare_wos_path = str(_p_w)

    _manual_files = []

    _uploaded_manual_files = get_widget_files(manual_compare_upload)

    if not _uploaded_manual_files:
        raise ValueError("Upload manual publication file(s).")

    for _name, _contents in _uploaded_manual_files:
        _p_m = _upload_dir / f"manual_{Path(_name).name}"
        _p_m.write_bytes(_contents)
        _manual_files.append(str(_p_m))

    _cfg = PipelineConfig(
        mode=_current_run_mode,
        run_uid=str(time.time_ns()),
        use_cache=False,
        page_size=int(page_size.value),
        sleep=float(sleep.value),

        compare_wos_file=_compare_wos_path,
        manual_dir=None,
        manual_files=_manual_files,
        compare_title_threshold=90,
        compare_year_window=int(compare_year_window.value),

        usr_query=None,
        max_records=None,
        start_date=None,
        end_date=None,
        doi_list_path=None,
        input_wos_csv=None,
        keywords_yml=None,
        do_keyword_filter=True,
        do_Contributor_check=False,
        do_merge_results=False,
        Contributor_csv=None,
    )

    _compare_pid, _compare_manifest_path, _compare_log_path = launch_config(_cfg)

    print(f"Started {_current_run_mode.value} with PID {_compare_pid}")
    print(f"Manifest path: {_compare_manifest_path}")
    print(f"Log path: {_compare_log_path}")

@app.cell
def stop_logic(
    is_running,
    mo,
    os,
    pid_is_running,
    runner_pid,
    set_is_running,
    set_runner_pid,
    signal,
    stop_run_btn,
    subprocess,
    time,
):
    mo.stop(not stop_run_btn.value)

    _pid = runner_pid()

    if _pid is None or not is_running():
        print("No active run.")
    else:
        try:
            if os.name == "nt":
                _res = subprocess.run(
                    ["taskkill", "/F", "/T", "/PID", str(_pid)],
                    capture_output=True,
                    text=True,
                    check=False,
                )

                if _res.returncode != 0:
                    print(f"taskkill failed for PID {_pid}")
                    if _res.stdout:
                        print(_res.stdout)
                    if _res.stderr:
                        print(_res.stderr)
                else:
                    print(f"Stop signal sent to PID {_pid}")

            else:
                try:
                    _pgid = os.getpgid(_pid)
                    os.killpg(_pgid, signal.SIGTERM)
                    time.sleep(2)

                    if pid_is_running(_pid):
                        os.killpg(_pgid, signal.SIGKILL)

                    print(f"Stop signal sent to PID {_pid}")

                except ProcessLookupError:
                    print(f"Process {_pid} was already gone.")

            time.sleep(1)

            if not pid_is_running(_pid):
                set_is_running(False)
                set_runner_pid(None)
                print(f"Stopped PID {_pid}")
            else:
                print(f"PID {_pid} is still running.")

        except Exception as e:
            print(f"Failed to stop PID {_pid}: {e}")

@app.cell
def read_progress_log(Path, auto_refresh, current_log_path):
    _ = auto_refresh.value

    def read_progress_log(max_lines=200):
        _log_path = current_log_path()
        if not _log_path:
            return []

        _p = Path(_log_path)
        if not _p.exists():
            return []

        try:
            _lines = _p.read_text(encoding="utf-8", errors="replace").splitlines()
            return _lines[-max_lines:]
        except Exception:
            return []

    return (read_progress_log,)


@app.cell
def results_ui(
    Path,
    auto_refresh,
    current_log_path,
    current_manifest_path,
    is_running,
    last_manifest,
    mo,
    pd,
    read_progress_log,
    runner_pid,
):
    _ = auto_refresh.value
    _manifest = last_manifest()

    _status_lines = []
    _status_lines.append(f"**Status:** {'Running' if is_running() else 'Idle'}")

    if runner_pid() is not None:
        _status_lines.append(f"- **PID:** `{runner_pid()}`")
    if current_log_path():
        _status_lines.append(f"- **Log:** `{current_log_path()}`")
    if current_manifest_path():
        _status_lines.append(f"- **Manifest path:** `{current_manifest_path()}`")

    _status_md = mo.md("\n".join(_status_lines))

    _log_lines = read_progress_log(max_lines=200)

    _progress_lines = []
    for _line in _log_lines:
        try:
            import json as _json
            _obj = _json.loads(_line)
            if isinstance(_obj, dict):
                if _obj.get("type") == "log" and "line" in _obj:
                    _progress_lines.append(_obj["line"])
                elif _obj.get("type") in {"fetch_query_start", "fetch_query_done", "fetch_doi_start", "fetch_doi_done", "filter_start", "filter_done", "compare_start", "compare_done"}:
                    _etype = _obj.get("type", "")
                    _progress_lines.append(f"[{_etype}]")
        except Exception:
            _progress_lines.append(_line)

    _progress_text = "\n".join(_progress_lines[-80:]) if _progress_lines else "No progress output yet."
    _progress_block = mo.vstack(
        [
            mo.md("### Progress"),
            mo.md("```text\n" + _progress_text + "\n```"),
        ]
    )

    if _manifest is None:
        _result_display = mo.vstack(
            [
                _status_md,
                mo.md("---"),
                _progress_block,
                mo.md("---"),
                mo.md("No completed results yet."),
            ]
        )
    else:
        _arts = _manifest.get("artifacts", {}) or {}
        _m_name = (_manifest.get("config", {}) or {}).get("mode", "unknown")

        _res_lines = [f"## Run Complete ({_m_name})"]
        for _k, _l in [
            ("run_dir", "Run Dir"),
            ("output_csv", "Fetched CSV"),
            ("keyword_filtered_csv", "Keyword Results"),
            ("name_checked_csv", "Name Results"),
            ("merged_csv", "Merged Results"),
            ("compare_csv", "Compare Result"),
        ]:
            if _arts.get(_k):
                _res_lines.append(f"- **{_l}:** `{_arts[_k]}`")

        _summary = mo.md("\n".join(_res_lines))

        def _get_preview(fpath):
            _p = Path(fpath)
            if not _p.exists():
                return mo.md(f"Not found: {fpath}")
            try:
                if _p.suffix == ".csv":
                    _df = pd.read_csv(_p, nrows=500)
                else:
                    _df = pd.read_excel(_p, nrows=500)

                return mo.ui.table(_df, pagination=True)
            except Exception as e:
                return mo.md(f"Error loading {_p.name}: {e}")

        _tabs = {}
        if _arts.get("keyword_filtered_csv"):
            _tabs["Keyword Results"] = _get_preview(_arts["keyword_filtered_csv"])
        if _arts.get("name_checked_csv"):
            _tabs["Name Results"] = _get_preview(_arts["name_checked_csv"])
        if _arts.get("merged_csv"):
            _tabs["Merged Results"] = _get_preview(_arts["merged_csv"])
        if _arts.get("output_csv"):
            _tabs["Full Result"] = _get_preview(_arts["output_csv"])
        if _arts.get("compare_csv"):
            _tabs["Compare Result"] = _get_preview(_arts["compare_csv"])

        _result_display = mo.vstack(
            [
                _status_md,
                mo.md("---"),
                _progress_block,
                mo.md("---"),
                _summary,
                mo.md("---"),
                mo.ui.tabs(_tabs) if _tabs else mo.md("No tables."),
            ]
        )

    _result_display
    return


if __name__ == "__main__":
    app.run()