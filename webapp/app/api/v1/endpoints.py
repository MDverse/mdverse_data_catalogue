from string import Template

from fastapi import APIRouter, Query

from ...database import ConnDep
from ...query_loader import load_query

router = APIRouter(prefix="/api/v1", tags=["All data"], include_in_schema=True)


@router.get("/datasets", summary="Retrieve datasets metadata in JSON format.")
def api_get_datasets(
    conn: ConnDep,
    q: str | None = Query(None, description="Search term for title or description"),
    limit: int = Query(10, ge=1, le=100, description="Maximum items to return"),
    offset: int = Query(0, ge=0, description="Number of items to skip"),
):
    """Retrieve datasets metadata in JSON format."""
    where_sql = "WHERE (d.title ILIKE ? OR d.description ILIKE ?)" if q else "WHERE 1=1"
    params = [f"%{q}%", f"%{q}%"] if q else []

    query_template = Template(load_query("explore_datasets.sql"))
    query = query_template.substitute(
        where_sql=where_sql,
        order_col="d.dataset_id",
        sort_dir="ASC",
        limit=limit,
        offset=offset,
    )

    df = conn.execute(query, params).fetchdf()
    total_count = int(df["total_count"].iloc[0]) if not df.empty else 0
    results = (
        df.drop(columns=["total_count"]).to_dict(orient="records")
        if not df.empty
        else []
    )

    return {
        "total_count": total_count,
        "limit": limit,
        "offset": offset,
        "results": results,
    }


@router.get("/file_types", summary="Retrieve file type statistics in JSON format.")
def api_get_file_types(
    conn: ConnDep,
    q: str | None = Query(None, description="Search term for file type or comment"),
    limit: int = Query(10, ge=1, le=100, description="Maximum items to return"),
    offset: int = Query(0, ge=0, description="Number of items to skip"),
):
    """Retrieve file type statistics and metrics in JSON format."""
    where_sql = (
        "WHERE (ft.file_type_label ILIKE ? OR ft.comment ILIKE ?)" if q else "WHERE 1=1"
    )
    params = [f"%{q}%", f"%{q}%"] if q else []

    query_template = Template(load_query("explore_files.sql"))
    query = query_template.substitute(
        where_sql=where_sql,
        order_col="COUNT(DISTINCT f.dataset_id)",
        sort_dir="DESC",
        limit=limit,
        offset=offset,
    )

    df = conn.execute(query, params).fetchdf()
    total_count = int(df["total_count"].iloc[0]) if not df.empty else 0
    results = (
        df.drop(columns=["total_count"]).to_dict(orient="records")
        if not df.empty
        else []
    )

    return {
        "total_count": total_count,
        "limit": limit,
        "offset": offset,
        "results": results,
    }


@router.get("/publications", summary="Retrieve scientific publications in JSON format.")
def api_get_publications(
    conn: ConnDep,
    q: str | None = Query(None, description="Search term for title, journal, or DOI"),
    limit: int = Query(10, ge=1, le=100, description="Maximum items to return"),
    offset: int = Query(0, ge=0, description="Number of items to skip"),
):
    """Retrieve scientific publications in JSON format."""
    where_sql = (
        "WHERE (p.title ILIKE ? OR p.journal ILIKE ? OR p.doi ILIKE ?)"
        if q
        else "WHERE 1=1"
    )
    params = [f"%{q}%", f"%{q}%", f"%{q}%"] if q else []

    query_template = Template(load_query("explore_publications.sql"))
    query = query_template.substitute(
        where_sql=where_sql,
        order_col="p.publication_id",
        sort_dir="ASC",
        limit=limit,
        offset=offset,
    )

    df = conn.execute(query, params).fetchdf()
    total_count = int(df["total_count"].iloc[0]) if not df.empty else 0
    results = (
        df.drop(columns=["total_count"]).to_dict(orient="records")
        if not df.empty
        else []
    )

    return {
        "total_count": total_count,
        "limit": limit,
        "offset": offset,
        "results": results,
    }


@router.get(
    "/ai_models", summary="Retrieve AI and Machine Learning models in JSON format."
)
def api_get_ai_models(
    conn: ConnDep,
    q: str | None = Query(
        None, description="Search term for model name, description, or tasks"
    ),
    limit: int = Query(10, ge=1, le=100, description="Maximum items to return"),
    offset: int = Query(0, ge=0, description="Number of items to skip"),
):
    """Retrieve AI and Machine Learning models in JSON format."""
    where_sql = (
        "WHERE (m.id_in_data_source ILIKE ? "
        "OR m.description ILIKE ? OR m.tasks ILIKE ?)"
        if q
        else "WHERE 1=1"
    )
    params = [f"%{q}%", f"%{q}%", f"%{q}%"] if q else []

    query_template = Template(load_query("explore_ai_models.sql"))
    query = query_template.substitute(
        where_sql=where_sql,
        order_col="m.ai_model_id",
        sort_dir="ASC",
        limit=limit,
        offset=offset,
    )

    df = conn.execute(query, params).fetchdf()
    total_count = int(df["total_count"].iloc[0]) if not df.empty else 0
    results = (
        df.drop(columns=["total_count"]).to_dict(orient="records")
        if not df.empty
        else []
    )

    return {
        "total_count": total_count,
        "limit": limit,
        "offset": offset,
        "results": results,
    }
