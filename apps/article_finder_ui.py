import marimo

__generated_with = "0.19.2"
app = marimo.App(css_file="custom.css")


@app.cell
def imports():
    import marimo as mo
    from pathlib import Path
    import sys
    import pandas as pd

    return Path, mo, pd, sys


@app.cell
def setup_pipeline(Path, sys):
    repo_root = Path(".").resolve()
    if str(repo_root) not in sys.path:
        sys.path.insert(0, str(repo_root))

    try:
        from af_core.pipeline import Mode, PipelineConfig, run_pipeline
    except ImportError:
        try:
            from pipeline import Mode, PipelineConfig, run_pipeline
        except ImportError:
            raise ImportError("Could not import 'pipeline.py'. Ensure it exists.")

    return Mode, PipelineConfig, repo_root, run_pipeline


@app.cell
def ui_state(mo):
    advanced_open, set_advanced_open = mo.state(False, allow_self_loops=True)
    last_manifest, set_last_manifest = mo.state(None)

    return advanced_open, set_advanced_open, last_manifest, set_last_manifest

@app.cell
def tab_state(mo):
    selected_tab, set_selected_tab = mo.state("Fetch")
    return selected_tab, set_selected_tab

@app.cell
def helpers():
    def get_widget_content(widget_object):
        files = widget_object.value
        if files and len(files) > 0:
            f = files[0]
            if hasattr(f, "name") and hasattr(f, "contents"):
                return f.name, f.contents
        return None, None

    return (get_widget_content,)


@app.cell
def ui_elements(advanced_open, mo, set_advanced_open):
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
        filetypes=[".csv", ".tsv"],
        label="WoS Results (csv)",
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

    do_Contributor_check = mo.ui.checkbox(value=True)

    do_Contributor_check_row = mo.hstack(
        [do_Contributor_check, mo.plain_text("Run name check (optional)")],
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

    advanced_btn = mo.ui.button(
        label=f"{'➖' if advanced_open() else '➕'} Advanced",
        kind="neutral",
        on_click=lambda _: set_advanced_open(not advanced_open()),
    )

    fetch_run_btn = mo.ui.run_button(label="Run Fetch", kind="success")
    filter_run_btn = mo.ui.run_button(label="Run Filter", kind="success")

    return (
        Contributor_header,
        Contributor_upload,
        FETCH_LABEL_TO_CODE,
        advanced_btn,
        do_Contributor_check,
        do_Contributor_check_row,
        doi_header,
        doi_upload,
        end_day_month_year,
        fetch_mode,
        fetch_run_btn,
        filter_run_btn,
        keywords_header,
        keywords_upload,
        max_records,
        page_size,
        sleep,
        start_day_month_year,
        use_cache,
        usr_query,
        wos_csv_upload,
    )


@app.cell
def main_layout(
    Contributor_header,
    Contributor_upload,
    advanced_btn,
    advanced_open,
    do_Contributor_check_row,
    doi_header,
    doi_upload,
    selected_tab,
    set_selected_tab,
    end_day_month_year,
    fetch_mode,
    fetch_run_btn,
    filter_run_btn,
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
            mo.right(fetch_run_btn),
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
            mo.right(fetch_run_btn),
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
            mo.md("**Upload an WoS file to run filtering.**"),
            wos_csv_upload,
            keywords_header,
            keywords_upload,
            Contributor_header,
            do_Contributor_check_row,
            Contributor_upload,
            mo.md("---"),
            mo.right(filter_run_btn),
        ],
        gap=0.5,
    )

    _main_tabs = mo.ui.tabs(
    {
        "Fetch": _fetch_panel,
        "Filter": _filter_panel,
    },
    value=selected_tab(),
    on_change=set_selected_tab,
)

    app_ui = mo.vstack(
        [
            mo.center(mo.md("# Article Finder")),
            _main_tabs,
            mo.md("---"),
            use_cache,
        ]
    )

    return app_ui


@app.cell
def display_ui(app_ui):
    app_ui
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
    max_records,
    mo,
    page_size,
    repo_root,
    run_pipeline,
    set_last_manifest,
    sleep,
    start_day_month_year,
    use_cache,
    usr_query,
):
    mo.stop(not fetch_run_btn.value)

    _fetch_label = fetch_mode.value
    _mode_code = FETCH_LABEL_TO_CODE.get(_fetch_label, "fetch_query")
    _current_run_mode = Mode(_mode_code)

    _upload_dir = repo_root / "runs" / "_uploads"
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
        do_Contributor_check=False,
        Contributor_csv=None,
    )

    def _on_event(e):
        _line = e.get("line") if isinstance(e, dict) else None
        if _line:
            print(_line)

    with mo.redirect_stdout():
        with mo.status.spinner(title=f"Running {_current_run_mode.value}..."):
            _manifest = run_pipeline(_cfg, repo_root=repo_root, on_event=_on_event)
            set_last_manifest(_manifest)


@app.cell
def run_filter_logic(
    Contributor_upload,
    Mode,
    Path,
    PipelineConfig,
    do_Contributor_check,
    filter_run_btn,
    get_widget_content,
    keywords_upload,
    mo,
    page_size,
    repo_root,
    run_pipeline,
    set_last_manifest,
    sleep,
    use_cache,
    wos_csv_upload,
):
    mo.stop(not filter_run_btn.value)

    _current_run_mode = Mode("filter")

    _upload_dir = repo_root / "runs" / "_uploads"
    _upload_dir.mkdir(parents=True, exist_ok=True)

    _w_n, _w_c = get_widget_content(wos_csv_upload)
    if not _w_n:
        raise ValueError("Upload WoS CSV.")

    _p_w = _upload_dir / f"wos_{Path(_w_n).name}"
    _p_w.write_bytes(_w_c)
    _wos_path = str(_p_w)

    _k_n, _k_c = get_widget_content(keywords_upload)
    if not _k_n:
        raise ValueError("Upload Keywords YAML.")

    _p_k = _upload_dir / f"keywords_{Path(_k_n).name}"
    _p_k.write_bytes(_k_c)
    _kw_path = str(_p_k)

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
        do_Contributor_check=_eff_c_check,
        Contributor_csv=_c_path,
    )

    def _on_event(e):
        _line = e.get("line") if isinstance(e, dict) else None
        if _line:
            print(_line)

    with mo.redirect_stdout():
        with mo.status.spinner(title=f"Running {_current_run_mode.value}..."):
            _manifest = run_pipeline(_cfg, repo_root=repo_root, on_event=_on_event)
            set_last_manifest(_manifest)


@app.cell
def results_ui(Path, last_manifest, mo, pd):
    _manifest = last_manifest()

    if _manifest is None:
        _result_display = mo.md("No results yet.")
    else:
        _arts = _manifest.get("artifacts", {}) or {}
        _m_name = (_manifest.get("config", {}) or {}).get("mode", "unknown")

        _res_lines = [f"## Run Complete ({_m_name})"]
        for _k, _l in [
            ("run_dir", "Run Dir"),
            ("output_csv", "Fetched CSV"),
            ("filtered_csv", "Filtered CSV"),
            ("Contributor_checked_csv", "Contributor CSV"),
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
        if _arts.get("filtered_csv"):
            _tabs["Filtered"] = _get_preview(_arts["filtered_csv"])
        if _arts.get("Contributor_checked_csv"):
            _tabs["Contributor"] = _get_preview(_arts["Contributor_checked_csv"])
        if _arts.get("output_csv"):
            _tabs["Full Result"] = _get_preview(_arts["output_csv"])

        _result_display = mo.vstack(
            [
                _summary,
                mo.md("---"),
                mo.ui.tabs(_tabs) if _tabs else mo.md("No tables."),
            ]
        )

    _result_display
    return


if __name__ == "__main__":
    app.run()