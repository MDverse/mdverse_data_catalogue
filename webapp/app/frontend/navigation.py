from datetime import datetime

import numpy as np
from bokeh.embed import components
from bokeh.models import ColumnDataSource, NumeralTickFormatter
from bokeh.plotting import figure
from bokeh.resources import CDN
from fastapi import APIRouter, Request
from fastapi.responses import HTMLResponse

from ..database import ConnDep, templates
from ..query_loader import load_query

router = APIRouter(tags=["Navigation Frontend"], include_in_schema=False)

COLORS = {
    "atlas": "#EB5757",
    "figshare": "#00FF00",
    "gpcrmd": "#F2C80F",
    "mdposit_cineca_node": "#46B3F3",
    "mdposit_inria_node": "#008CEE",
    "mdposit_mmb_node": "#0641C8",
    "nomad": "#E044A7",
    "zenodo": "#0AAC00",
}


def _make_cumulative_plot(df_history, target: str = "datasets"):
    """Build Bokeh cumulative plot for datasets or files by repository."""
    if df_history.empty:
        p = figure(height=450, sizing_mode="scale_width")
        return p

    repositories = sorted(df_history["repository"].dropna().unique().tolist())
    current_year = datetime.now().year
    all_years = list(range(2012, current_year + 1))
    data = {"year": [str(y) for y in all_years]}

    count_col = "files_count" if target == "files" else "datasets_count"

    for repo in repositories:
        sub = df_history[df_history["repository"] == repo]
        stats = dict(zip(sub["year"], sub[count_col]))
        counts = np.array([stats.get(y, 0) for y in all_years])
        data[repo] = np.cumsum(counts)

    source = ColumnDataSource(data=data)

    p = figure(
        x_range=data["year"],
        y_axis_type="log",
        height=450,
        sizing_mode="scale_width",
        title=f"Cumulative number of {target} by year and data repository",
        tooltips=[
            ("Year", "@year"),
            ("Data repository", "$name"),
            (f"Number of {target}", "$snap_y{0,0}"),
        ],
        tools="hover,box_zoom,reset,save",
        background_fill_color="#fafafa",
    )

    for repo in repositories:
        color = COLORS.get(repo, "#333333")
        p.line(
            x="year",
            y=repo,
            width=3,
            source=source,
            color=color,
            legend_label=repo,
            name=repo,
        )
        p.scatter(
            x="year",
            y=repo,
            size=8,
            source=source,
            fill_color="white",
            line_color=color,
            line_width=2,
            legend_label=repo,
            name=repo,
        )

    p.toolbar.active_drag = None
    p.xaxis.axis_label = "Year"
    p.yaxis.axis_label = f"Number of {target} (log)"
    p.yaxis.formatter = NumeralTickFormatter(format="0,0")

    p.title.text_font_size = "13pt"
    p.xaxis.axis_label_text_font_size = "11pt"
    p.yaxis.axis_label_text_font_size = "11pt"

    p.legend.location = "top_left"
    p.legend.background_fill_alpha = 0.3
    p.legend.border_line_color = None
    p.legend.label_text_font_size = "9pt"
    p.legend.click_policy = "hide"

    return p


@router.get("/", response_class=HTMLResponse)
def page_index(request: Request, conn: ConnDep):
    """Render home landing page with general catalogue statistics."""
    query = load_query("count_sources.sql")
    row = conn.execute(query).fetchone()

    banner_stats = {
        "datasets": f"{row[0]:,}" if row else "0",
        "files": f"{row[1]:,}" if row else "0",
        "publications": f"{row[2]:,}" if row else "0",
        "ai_models": f"{row[3]:,}" if row else "0",
        "sources": f"{row[4]:,}" if row else "0",
        "structures": f"{row[5]:,}" if row else "0",
        "trajectories": f"{row[6]:,}" if row else "0",
        "last_update": "March 2026",
    }

    return templates.TemplateResponse(
        request=request,
        name="index.html",
        context={"banner_stats": banner_stats},
    )


@router.get("/documentation", response_class=HTMLResponse)
def page_documentation(request: Request, conn: ConnDep):
    """Render documentation page with database statistics and Bokeh charts."""
    sources_query = load_query("summary_by_sources.sql")
    results_df = conn.execute(sources_query).fetchdf()
    results = results_df.to_dict(orient="records")

    total_datasets = (
        int(results_df["number_of_datasets"].sum()) if not results_df.empty else 0
    )
    total_files = int(results_df["total_files"].sum()) if not results_df.empty else 0
    total_size = (
        float(results_df["total_size_in_GB_non_zip_and_zip_files"].sum())
        if not results_df.empty
        else 0
    )
    first_dt = (
        str(results_df["first_dataset"].min()).split("T")[0]
        if not results_df.empty
        else "-"
    )
    last_dt = (
        str(results_df["last_dataset"].max()).split("T")[0]
        if not results_df.empty
        else "-"
    )

    total_count = {
        "number_of_datasets": f"{total_datasets:,}",
        "total_files": f"{total_files:,}",
        "first_dataset": first_dt,
        "last_dataset": last_dt,
        "total_size_in_GB_non_zip_and_zip_files": f"{total_size:,.0f}",
    }

    # Generate Bokeh cumulative plots
    charts_query = load_query("evolution_by_sources.sql")
    hist_df = conn.execute(charts_query).fetchdf()

    p_files = _make_cumulative_plot(hist_df, target="files")
    p_datasets = _make_cumulative_plot(hist_df, target="datasets")

    files_script, files_div = components(p_files)
    datasets_script, datasets_div = components(p_datasets)

    return templates.TemplateResponse(
        request=request,
        name="documentation.html",
        context={
            "results": results,
            "total_count": total_count,
            "bokeh_resources": CDN.render(),
            "files_plot_script": files_script,
            "files_plot_div": files_div,
            "datasets_plot_script": datasets_script,
            "datasets_plot_div": datasets_div,
        },
    )


@router.get("/about", response_class=HTMLResponse)
def page_about(request: Request):
    """Render the about information page."""
    return templates.TemplateResponse(request=request, name="about.html")


@router.get("/advanced-search", response_class=HTMLResponse)
def page_advanced_search(request: Request):
    """Render the advanced search page with default empty context."""
    return templates.TemplateResponse(
        request=request,
        name="explore.html",
        context={
            "tab": "datasets",
            "results": [],
            "page": 1,
            "limit": 5,
            "total_pages": 1,
            "total_count": 0,
            "sidebar_filters": {},
        },
    )
