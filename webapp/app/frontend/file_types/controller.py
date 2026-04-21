"""Endpoints for the page: file_types"""

from fastapi import APIRouter, Request, Response
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.templating import Jinja2Templates

from . import service

router = APIRouter(
    prefix="",
    tags=["frontend"],
)

templates = Jinja2Templates(directory="templates")


@router.get("/file_types", response_class=HTMLResponse)
async def file_types_table(request: Request):
    file_type_stats_summary = service.get_file_types_stats()
    return templates.TemplateResponse(
        "file_types_page.html",
        {
            "request": request,
            "file_type_stats_summary": file_type_stats_summary,
        }
    )

# Render the button to downlaod all files for a given file type.
# Endpoint triggered by HTMX.
@router.get("/file_types/{file_type}/download_info", response_class=HTMLResponse)
async def display_button_to_download_file_list(request: Request, file_type: str):
    return templates.TemplateResponse(
        "file_types_download_info.html",
        {
            "request": request,
            "file_type": file_type,
        }
    )

# Download the list of files for a given file type.
@router.get("/file_types/{file_type}/download_list/")
async def download_file_list(file_type: str):
    df = service.get_list_of_files_for_a_file_type(file_type)
    tsv_data = df.to_csv(index=False, sep="\t")
    headers = {
        "Content-Disposition": f"attachment; filename=mdverse_{file_type}.tsv"
    }
    return Response(content=tsv_data, media_type="text/tsv", headers=headers)


# ============================================================================
# GRO files
# ============================================================================
@router.get("/file_types/gro", response_class=HTMLResponse)
async def display_gro_files_page(request: Request):
    return templates.TemplateResponse(
        "gro_files_page.html",
        {
            "request": request,
        }
    )

@router.get("/file_types/gro/datatables", response_class=JSONResponse)
async def get_gro_files_for_datatables(request: Request, dataset_id: int | None = None):
    """
    Get GRO files data for DataTables.

    See:
    - https://datatables.net/manual/server-side
    - https://blog.stackpuz.com/create-an-api-for-datatables-with-fastapi/

    Parameters
    ----------
    request : Request
        DataTables request parameters + optional dataset id.

    Returns
    -------
    dict
        JSON dictionnary for DataTables.
    """
    print("Hello from /files/topologie/")
    print("dataset_id", dataset_id)
    params = request.query_params.get
    sort_column_name = "dataset_origin"
    if params("order[0][column]"):
        sort_column_idx = params("order[0][column]")
        sort_column_name = params(f"columns[{sort_column_idx}][data]")
    sort_direction = "asc"
    if params("order[0][dir]") == "desc":
        sort_direction = "desc"
    number_of_top_files_total = len(service.get_gro_files_for_datatables(dataset_id=dataset_id))
    number_of_top_files_filtered = len(service.get_gro_files_for_datatables(
        dataset_id=dataset_id,
        search=params("search[value]"),
    ))
    top_files = service.get_gro_files_for_datatables(
        dataset_id=dataset_id,
        sort_column_name=sort_column_name,
        sort_direction=sort_direction,
        start=params("start"),
        length=params("length"),
        search=params("search[value]"),
    )
    # Serialize SQLmodel results to JSON
    data = [ row._mapping for row in top_files ]
    return {
        "draw": params("draw"),
        "recordsTotal": number_of_top_files_total,
        "recordsFiltered": number_of_top_files_filtered,
        "data": data,
    }

# ============================================================================
# MDP files
# ============================================================================
@router.get("/file_types/mdp", response_class=HTMLResponse)
async def display_mdp_files_page(request: Request):
    return templates.TemplateResponse(
        "mdp_files_page.html",
        {
            "request": request,
        }
    )

@router.get("/file_types/mdp/datatables", response_class=JSONResponse)
async def get_mdp_files_for_datatables(request: Request, dataset_id: int | None = None):
    """
    Get MDP files data for DataTables.

    See:
    - https://datatables.net/manual/server-side
    - https://blog.stackpuz.com/create-an-api-for-datatables-with-fastapi/

    Parameters
    ----------
    request : Request
        DataTables request parameters + optional dataset id.

    Returns
    -------
    dict
        JSON dictionnary for DataTables.
    """
    print("dataset_id", dataset_id)
    params = request.query_params.get
    sort_column_name = "dataset_origin"
    if params("order[0][column]"):
        sort_column_idx = params("order[0][column]")
        sort_column_name = params(f"columns[{sort_column_idx}][data]")
    sort_direction = "asc"
    if params("order[0][dir]") == "desc":
        sort_direction = "desc"
    number_of_mdp_files_total = len(service.get_mdp_files_for_datatables(dataset_id=dataset_id))
    number_of_mdp_files_filtered = len(service.get_mdp_files_for_datatables(
        dataset_id=dataset_id,
        search=params("search[value]"),
    ))
    mdp_files = service.get_mdp_files_for_datatables(
        dataset_id=dataset_id,
        sort_column_name=sort_column_name,
        sort_direction=sort_direction,
        start=params("start"),
        length=params("length"),
        search=params("search[value]"),
    )
    # Serialize SQLmodel results to JSON
    data = [ row._mapping for row in mdp_files ]
    return {
        "draw": params("draw"),
        "recordsTotal": number_of_mdp_files_total,
        "recordsFiltered": number_of_mdp_files_filtered,
        "data": data,
    }

# ============================================================================
# XTC files
# ============================================================================
@router.get("/file_types/xtc/datatables", response_class=JSONResponse)
async def get_xtc_files_for_datatables(request: Request, dataset_id: int | None = None):
    """
    Get XTC files data for DataTables.

    See:
    - https://datatables.net/manual/server-side
    - https://blog.stackpuz.com/create-an-api-for-datatables-with-fastapi/

    Parameters
    ----------
    request : Request
        DataTables request parameters + optional dataset id.

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
    number_of_mdp_files_total = len(service.get_xtc_files_for_datatables(dataset_id=dataset_id))
    number_of_mdp_files_filtered = len(service.get_xtc_files_for_datatables(
        dataset_id=dataset_id,
        search=params("search[value]"),
    ))
    mdp_files = service.get_xtc_files_for_datatables(
        dataset_id=dataset_id,
        sort_column_name=sort_column_name,
        sort_direction=sort_direction,
        start=params("start"),
        length=params("length"),
        search=params("search[value]"),
    )
    # Serialize SQLmodel results to JSON
    data = [ row._mapping for row in mdp_files ]
    return {
        "draw": params("draw"),
        "recordsTotal": number_of_mdp_files_total,
        "recordsFiltered": number_of_mdp_files_filtered,
        "data": data,
    }
