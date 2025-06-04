from bokeh.embed import components
from fastapi import APIRouter, Request
from fastapi.responses import HTMLResponse
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates

from . import service


router = APIRouter(
    prefix="",
    tags=["frontend"],
)
templates = Jinja2Templates(directory="templates")

# ============================================================================
# Endpoints for the page: index.html
# ============================================================================

@router.get("/", response_class=HTMLResponse)
async def read_index(request: Request):
    # Generate the wordcloud image.
    service.generate_title_wordcloud()

    # Get the data from query
    datasets_stats_results, datasets_stats_total_count, sum_of_analysed_files = service.get_dataset_origin_summary()

    # Create both Bokeh plots.
    files_plot = service.create_files_plot()
    datasets_plot = service.create_datasets_plot()

    # Get the script and div for each plot.
    files_plot_script, files_plot_div = components(files_plot)
    datasets_plot_script, datasets_plot_div = components(datasets_plot)

    # Pass it to the template
    return templates.TemplateResponse(
        "index_page.html",
        {
            "request": request,
            "results": datasets_stats_results,
            "total_count": datasets_stats_total_count,
            "analysed_files_count": sum_of_analysed_files,
            "files_plot_script": files_plot_script,
            "files_plot_div": files_plot_div,
            "datasets_plot_script": datasets_plot_script,
            "datasets_plot_div": datasets_plot_div,
        }
    )
