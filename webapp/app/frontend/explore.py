from string import Template

from fastapi import APIRouter, Query, Request
from fastapi.responses import HTMLResponse

from ..database import ConnDep, templates
from ..query_loader import load_query

router = APIRouter(tags=["Explore Frontend"], include_in_schema=False)


# Map tabs to their SQL file and allowed sorting columns
TAB_CONFIGS = {
    "datasets": {
        "query_file": "explore_datasets.sql",
        "search_fields": [
            "d.title",
            "d.description",
            "d.keywords",
            "d.id_in_data_source",
        ],
        "sorts": {
            "title": "d.title",
            "repository": "d.data_source_label",
            "created": "d.date_created",
            "files": "COUNT(DISTINCT f.file_id)",
            "publications": "COUNT(DISTINCT dpl.publication_id)",
            "models": "COUNT(DISTINCT dml.ai_model_id)",
        },
        "default_sort": "d.date_created",
    },
    "publications": {
        "query_file": "explore_publications.sql",
        "search_fields": ["p.title", "p.abstract", "p.journal", "p.doi"],
        "sorts": {
            "title": "p.title",
            "journal": "p.journal",
            "year": "p.year",
            "doi": "p.doi",
            "datasets": "COUNT(DISTINCT dpl.dataset_id)",
        },
        "default_sort": "p.year",
    },
    "files": {
        "query_file": "explore_file_types.sql",
        "search_fields": ["ft.file_type_label", "ft.comment"],
        "sorts": {
            "type": "ft.file_type_label",
            "comment": "ft.comment",
            "datasets": "number_of_datasets",
            "files": "number_of_files",
            "size": "total_size_in_GB",
        },
        "default_sort": "number_of_datasets",
    },
    "ai_models": {
        "query_file": "explore_ai_models.sql",
        "search_fields": ["m.id_in_data_source", "m.tasks", "m.description"],
        "sorts": {
            "name": "m.id_in_data_source",
            "tasks": "m.tasks",
            "params": "m.number_of_parameters",
            "created": "m.date_created",
            "datasets": "COUNT(DISTINCT dml.dataset_id)",
        },
        "default_sort": "m.date_created",
    },
}


@router.get("/explore", response_class=HTMLResponse)
def page_explore(
    request: Request,
    conn: ConnDep,
    tab: str = Query("datasets", pattern="^(datasets|publications|files|ai_models)$"),
    q: str | None = None,
    sort_by: str | None = None,
    sort_dir: str = Query("desc", pattern="^(asc|desc)$"),
    page: int = Query(1, ge=1),
    limit: int = Query(5, ge=1, le=100),
    pdb_id: str | None = None,
    uniprot_id: str | None = None,
    software: str | None = None,
    temperature: str | None = None,
    force_field: str | None = None,
    water_model: str | None = None,
    timestep: str | None = None,
    simulation_time: str | None = None,
    author_orcid: str | None = None,
    author_name: str | None = None,
    molecule_type: str | None = None,
):
    """Render the main explore page loading SQL queries from dedicated files."""
    offset = (page - 1) * limit
    results = []
    total_count = 0

    if tab in TAB_CONFIGS:
        config = TAB_CONFIGS[tab]
        order_col = config["sorts"].get(sort_by, config["default_sort"])
        effective_sort_dir = (
            sort_dir.upper() if sort_by else config.get("default_dir", sort_dir.upper())
        )

        if q:
            conditions = [f"{field} ILIKE ?" for field in config["search_fields"]]
            where_sql = f"WHERE ({' OR '.join(conditions)})"
            params = [f"%{q}%"] * len(config["search_fields"])
        else:
            where_sql = "WHERE 1=1"
            params = []

        query_template = Template(load_query(config["query_file"]))
        query = query_template.substitute(
            where_sql=where_sql,
            order_col=order_col,
            sort_dir=effective_sort_dir,
            limit=limit,
            offset=offset,
        )
        results_df = conn.execute(query, params).fetchdf()

        if not results_df.empty:
            total_count = int(results_df["total_count"].iloc[0])
            results = results_df.drop(columns=["total_count"]).to_dict(orient="records")

    total_pages = max(1, (total_count + limit - 1) // limit)

    return templates.TemplateResponse(
        request=request,
        name="explore.html",
        context={
            "tab": tab,
            "q": q or "",
            "sort_by": sort_by or "",
            "sort_dir": sort_dir,
            "results": results,
            "page": page,
            "limit": limit,
            "total_pages": total_pages,
            "total_count": total_count,
            "sidebar_filters": {
                "pdb_id": pdb_id or "",
                "uniprot_id": uniprot_id or "",
                "software": software or "",
                "temperature": temperature or "",
                "force_field": force_field or "",
                "water_model": water_model or "",
                "timestep": timestep or "",
                "simulation_time": simulation_time or "",
                "author_orcid": author_orcid or "",
                "author_name": author_name or "",
                "molecule_type": molecule_type or "",
            },
        },
    )
