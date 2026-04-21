from fastapi import APIRouter, Request
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.templating import Jinja2Templates

from . import service

router = APIRouter(
    prefix="",
    tags=["frontend"],
)

templates = Jinja2Templates(directory="templates")

# ============================================================================
# Endpoints for the page: datasets.html
# ============================================================================

@router.get("/datasets", response_class=HTMLResponse)
async def get_datasets(request: Request):
    # Get the list of all datasets (with related data loaded)
    datasets = service.get_all_datasets()
    # Pass the list as "datasets" to the template.
    return templates.TemplateResponse(
        "datasets_page.html",
        {
            "request": request,
            "datasets": datasets,
        }
    )


@router.get("/datasets/datatables", response_class=JSONResponse)
async def get_datasets_for_datatables(request: Request):
    """
    Get all datasets for DataTables.

    See:
    - https://datatables.net/manual/server-side
    - https://blog.stackpuz.com/create-an-api-for-datatables-with-fastapi/

    Parameters
    ----------
    request : Request
        DataTables request parameters

    Returns
    -------
    dict
        JSON dictionnary for DataTables.
    """
    params = request.query_params.get
    sort_column_name = "dataset_origin"
    if params("order[0][column]"):
        sort_column_idx = params("order[0][column]")
        sort_column_name = params(f"columns[{sort_column_idx}][data]")
    sort_direction = "asc"
    if params("order[0][dir]") == "desc":
        sort_direction = "desc"
    number_of_datasets_total = len(service.get_all_datasets_for_datatables())
    number_of_datasets_filtered = len(service.get_all_datasets_for_datatables(
        search=params("search[value]"),
    ))
    datasets = service.get_all_datasets_for_datatables(
        sort_column_name=sort_column_name,
        sort_direction=sort_direction,
        start=params("start"),
        length=params("length"),
        search=params("search[value]"),
    )
    # Serialize SQLmodel results to JSON
    data = [row._mapping for row in datasets]
    return {
        "draw": params("draw"),
        "recordsTotal": number_of_datasets_total,
        "recordsFiltered": number_of_datasets_filtered,
        "data": data,
    }

@router.get("/datasets/{dataset_id}", response_class=HTMLResponse)
async def get_dataset_info(
    request: Request,
    dataset_id: int
    ):
    dataset, _, _ = service.get_dataset_info_by_id(dataset_id)
    return templates.TemplateResponse(
        "dataset_info.html",
        {"request": request, "dataset": dataset}
    )

@router.get("/datasets/{dataset_id}/files", response_class=HTMLResponse)
async def get_dataset_files(request: Request, dataset_id: int):
    dataset, total_files, analysed_files = service.get_dataset_info_by_id(dataset_id)
    return templates.TemplateResponse(
        "dataset_files_page.html", {"request": request, "dataset": dataset, "total_files": total_files, "analysed_files": analysed_files}
    )

@router.get("/datasets/{dataset_id}/files/all", response_class=HTMLResponse)
async def get_dataset_all_files(request: Request, dataset_id: int):
    all_files = service.get_all_files_from_dataset(dataset_id)
    return templates.TemplateResponse(
        "dataset_files_all_table.html", {"request": request, "all_files": all_files}
        )

@router.get("/datasets/{dataset_id}/files/gro", response_class=HTMLResponse)
async def get_dataset_gro_files(request: Request, dataset_id: int):
    return templates.TemplateResponse(
        "gro_files_table.html", {"request": request, "dataset_id": dataset_id}
    )

@router.get("/datasets/{dataset_id}/files/mdp", response_class=HTMLResponse)
async def get_dataset_mdp_files(request: Request, dataset_id: int):
    return templates.TemplateResponse(
        "mdp_files_table.html", {"request": request, "dataset_id": dataset_id}
        )

@router.get("/datasets/{dataset_id}/files/xtc", response_class=HTMLResponse)
async def get_dataset_xtc_files(request: Request, dataset_id: int):
    return templates.TemplateResponse(
        "xtc_files_table.html", {"request": request, "dataset_id": dataset_id}
    )
