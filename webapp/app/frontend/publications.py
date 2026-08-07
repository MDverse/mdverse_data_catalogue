from http.client import HTTPException

from fastapi import APIRouter, Request
from fastapi.responses import HTMLResponse

from ..database import ConnDep, templates

router = APIRouter(tags=["Publications Frontend"], include_in_schema=False)


@router.get("/publications/{publication_id}", response_class=HTMLResponse)
def page_publication_detail(request: Request, publication_id: int, conn: ConnDep):
    """Render the publication detail page with linked authors, datasets, and models."""
    # 1. Fetch Publication details
    query_pub = "SELECT * FROM publications WHERE publication_id = ?"
    pub_res = (
        conn.execute(query_pub, [publication_id]).fetchdf().to_dict(orient="records")
    )

    if not pub_res:
        raise HTTPException(status_code=404, detail="Publication not found")

    publication = pub_res[0]

    # 2. Fetch Authors
    query_authors = """
        SELECT p.*
        FROM persons p
        JOIN authors_publications_link apl ON p.person_id = apl.person_id
        WHERE apl.publication_id = ?
    """
    authors = (
        conn.execute(query_authors, [publication_id])
        .fetchdf()
        .to_dict(orient="records")
    )

    # 3. Fetch Linked Datasets
    query_datasets = """
        SELECT d.*
        FROM datasets d
        JOIN datasets_publications_link dpl ON d.dataset_id = dpl.dataset_id
        WHERE dpl.publication_id = ?
    """
    datasets = (
        conn.execute(query_datasets, [publication_id])
        .fetchdf()
        .to_dict(orient="records")
    )

    # 4. Fetch Linked AI Models
    query_models = """
        SELECT m.*
        FROM ai_models m
        JOIN publications_models_link pml ON m.ai_model_id = pml.ai_model_id
        WHERE pml.publication_id = ?
    """
    models = (
        conn.execute(query_models, [publication_id]).fetchdf().to_dict(orient="records")
    )

    return templates.TemplateResponse(
        request=request,
        name="publication_detail.html",
        context={
            "publication": publication,
            "authors": authors,
            "datasets": datasets,
            "models": models,
        },
    )
