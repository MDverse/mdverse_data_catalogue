from fastapi import APIRouter, Request
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.templating import Jinja2Templates

from ...dependencies import SessionDep
from . import service

router = APIRouter(
    prefix="",
    tags=["frontend"],
)

templates = Jinja2Templates(directory="webapp/templates")

# ============================================================================
# Endpoints for the page: datasets.html
# ============================================================================


@router.get("/datasets", response_class=HTMLResponse)
async def get_datasets(request: Request, session: SessionDep):
    # Get the list of all datasets (with related data loaded)
    datasets = service.get_all_datasets(session)
    # Pass the list as "datasets" to the template.
    return templates.TemplateResponse(
        request=request,
        name="datasets_page.html",
        context={"datasets": datasets},
    )


@router.get("/datasets/datatables", response_class=JSONResponse)
async def get_datasets_for_datatables(request: Request, session: SessionDep):
    """
    Get all datasets for DataTables.

    See:
    - https://datatables.net/manual/server-side

    Parameters
    ----------
    request : Request
        DataTables request parameters

    Returns
    -------
    dict
        JSON dictionary for DataTables.
    """
    params = request.query_params.get
    sort_column_name = "date_created"
    if params("order[0][column]"):
        sort_column_idx = params("order[0][column]")
        sort_column_name = params(f"columns[{sort_column_idx}][data]")
    sort_direction = "desc"
    if params("order[0][dir]") == "desc":
        sort_direction = "desc"
    number_of_datasets_total = len(service.get_all_datasets_for_datatables(session))
    number_of_datasets_filtered = len(
        service.get_all_datasets_for_datatables(
            session,
            search=params("search[value]"),
        )
    )
    datasets = service.get_all_datasets_for_datatables(
        session,
        sort_column_name=sort_column_name,
        sort_direction=sort_direction,
        start=params("start"),
        length=params("length"),
        search=params("search[value]"),
    )
    # Serialize SQLmodel results to JSON.
    data = [row._mapping for row in datasets]
    return {
        "draw": params("draw"),
        "recordsTotal": number_of_datasets_total,
        "recordsFiltered": number_of_datasets_filtered,
        "data": data,
    }


@router.get("/datasets/{dataset_id}", response_class=HTMLResponse)
async def get_dataset_info(request: Request, session: SessionDep, dataset_id: int):
    dataset, total_files = service.get_dataset_info_by_id(session, dataset_id)
    all_files = service.get_all_files_from_dataset(session, dataset_id)
    return templates.TemplateResponse(
        request=request,
        name="dataset_info.html",
        context={
            "dataset": dataset,
            "total_files": total_files,
            "all_files": all_files,
        },
    )
