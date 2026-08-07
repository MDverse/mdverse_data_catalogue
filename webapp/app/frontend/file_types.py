from string import Template

from fastapi import APIRouter, Request, Response
from fastapi.responses import HTMLResponse

from ..database import ConnDep, templates
from ..query_loader import load_query

router = APIRouter(tags=["File Types Frontend"], include_in_schema=False)


@router.get("/file_types", response_class=HTMLResponse)
def page_file_types_summary(request: Request, conn: ConnDep):
    """Render file types summary page with aggregated statistics."""
    query_template = Template(load_query("explore_files.sql"))
    query = query_template.substitute(
        where_sql="WHERE 1=1",
        order_col="COUNT(DISTINCT f.dataset_id)",
        sort_dir="DESC",
        limit=1000,
        offset=0,
    )
    df = conn.execute(query).fetchdf()
    stats_summary = (
        df.drop(columns=["total_count"]).to_dict(orient="records")
        if not df.empty
        else []
    )
    return templates.TemplateResponse(
        request=request,
        name="file_types_page.html",
        context={"file_type_stats_summary": stats_summary},
    )


@router.get("/file_types/{file_type}/download_list/")
def download_file_list(conn: ConnDep, file_type: str):
    """Stream TSV list containing files for a specific file format."""
    query = load_query("download_file_type.sql")
    df = conn.execute(query, [file_type]).fetchdf()
    tsv_data = df.to_csv(index=False, sep="\t")
    filename = f"mdverse_{file_type}.tsv"
    headers = {"Content-Disposition": f"attachment; filename={filename}"}
    return Response(content=tsv_data, media_type="text/tsv", headers=headers)
