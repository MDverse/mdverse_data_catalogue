from bokeh.embed import components
from fastapi import APIRouter, Request
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates

from ..dependencies import SessionDep
from . import service

router = APIRouter(
    prefix="",
    tags=["frontend"],
)
templates = Jinja2Templates(directory="webapp/templates")

# ============================================================================
# Endpoints for the page: index.html
# ============================================================================


@router.get("/", response_class=HTMLResponse)
async def read_index(request: Request, session: SessionDep):
    # Get the data from query
    datasets_stats_results, datasets_stats_total_count, home_page_banner_stats = (
        service.get_dataset_origin_summary(session)
    )

    # Create both Bokeh plots.
    files_plot = service.create_files_plot(session)
    datasets_plot = service.create_datasets_plot(session)

    # Get the script and div for each plot.
    files_plot_script, files_plot_div = components(files_plot)
    datasets_plot_script, datasets_plot_div = components(datasets_plot)

    # Pass it to the template
    return templates.TemplateResponse(
        request=request,
        name="index_page.html",
        context={
            "request": request,
            "results": datasets_stats_results,
            "total_count": datasets_stats_total_count,
            "banner_stats": home_page_banner_stats,
            "files_plot_script": files_plot_script,
            "files_plot_div": files_plot_div,
            "datasets_plot_script": datasets_plot_script,
            "datasets_plot_div": datasets_plot_div,
        },
    )


@router.get("/about", response_class=HTMLResponse)
async def show_about_page(request: Request):
    return templates.TemplateResponse(
        request=request,
        name="about_page.html",
    )
