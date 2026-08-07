from fastapi import APIRouter, Request
from fastapi.responses import HTMLResponse

from ..database import ConnDep, templates

router = APIRouter(tags=["AI Models Frontend"], include_in_schema=False)


@router.get("/ai_models/{ai_model_id}", response_class=HTMLResponse)
def page_ai_model_detail(request: Request, ai_model_id: int, conn: ConnDep):
    """Render the detail page for a specific AI model with linked metadata."""
    # Fetch core AI model metadata
    model_res = (
        conn.execute("SELECT * FROM ai_models WHERE ai_model_id = ?", [ai_model_id])
        .fetchdf()
        .to_dict(orient="records")
    )
    if not model_res:
        # Fallback to explore view if model ID is not found
        return templates.TemplateResponse(
            request=request,
            name="explore.html",
            context={
                "tab": "ai_models",
                "results": [],
                "page": 1,
                "total_pages": 1,
                "total_count": 0,
                "sidebar_filters": {},
            },
        )

    model = model_res[0]
    # Fetch associated authors (persons)
    authors = (
        conn.execute(
            "SELECT p.* FROM persons p JOIN models_authors_link mal "
            "ON p.person_id = mal.person_id WHERE mal.ai_model_id = ?",
            [ai_model_id],
        )
        .fetchdf()
        .to_dict(orient="records")
    )
    # Fetch associated datasets
    datasets = (
        conn.execute(
            "SELECT d.* FROM datasets d JOIN datasets_models_link dml "
            "ON d.dataset_id = dml.dataset_id WHERE dml.ai_model_id = ?",
            [ai_model_id],
        )
        .fetchdf()
        .to_dict(orient="records")
    )
    # Fetch associated publications
    publications = (
        conn.execute(
            "SELECT pub.* FROM publications pub JOIN publications_models_link pml "
            "ON pub.publication_id = pml.publication_id WHERE pml.ai_model_id = ?",
            [ai_model_id],
        )
        .fetchdf()
        .to_dict(orient="records")
    )
    return templates.TemplateResponse(
        request=request,
        name="ai_model_detail.html",
        context={
            "model": model,
            "authors": authors,
            "datasets": datasets,
            "publications": publications,
        },
    )
