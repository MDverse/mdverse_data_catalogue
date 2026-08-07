import json
from http.client import HTTPException

from fastapi import APIRouter, Request
from fastapi.responses import HTMLResponse, Response

from ..database import ConnDep, templates

router = APIRouter(tags=["Datasets Frontend"], include_in_schema=False)


@router.get("/datasets/{dataset_id}", response_class=HTMLResponse)
def page_dataset_detail(request: Request, dataset_id: int, conn: ConnDep):
    """Render the detail page for a dataset with linked metadata+annotations."""
    # 1. Fetch target dataset information
    query_dataset = "SELECT * FROM datasets WHERE dataset_id = ?"
    dataset_res = (
        conn.execute(query_dataset, [dataset_id]).fetchdf().to_dict(orient="records")
    )
    if not dataset_res:
        # Fallback to explore view if dataset is not found
        return templates.TemplateResponse(
            request=request,
            name="explore.html",
            context={
                "tab": "datasets",
                "results": [],
                "page": 1,
                "total_pages": 1,
                "total_count": 0,
                "sidebar_filters": {},
            },
        )
    dataset = dataset_res[0]

    # 2. Fetch associated files
    query_files = "SELECT * FROM files WHERE dataset_id = ?"
    files = conn.execute(query_files, [dataset_id]).fetchdf().to_dict(orient="records")

    # 3. Fetch associated authors (persons)
    query_authors = (
        "SELECT p.* FROM persons p JOIN datasets_authors_link dal "
        "ON p.person_id = dal.person_id WHERE dal.dataset_id = ?"
    )
    authors = (
        conn.execute(query_authors, [dataset_id]).fetchdf().to_dict(orient="records")
    )

    # 4. Fetch associated publications
    query_publications = (
        "SELECT pub.* FROM publications pub JOIN datasets_publications_link dpl "
        "ON pub.publication_id = dpl.publication_id WHERE dpl.dataset_id = ?"
    )
    publications = (
        conn.execute(query_publications, [dataset_id])
        .fetchdf()
        .to_dict(orient="records")
    )

    # 5. Fetch associated AI models
    query_models = (
        "SELECT m.* FROM ai_models m JOIN datasets_models_link dml "
        "ON m.ai_model_id = dml.ai_model_id WHERE dml.dataset_id = ?"
    )
    models = (
        conn.execute(query_models, [dataset_id]).fetchdf().to_dict(orient="records")
    )

    # 6. Fetch MD Annotations (NATOMS, STEMP, STIME, SOFTNAME, SOFTVERS, FFM...)
    query_annotations = (
        "SELECT a.category_label, a.value, a.value_extra, a.provenance_label, a.comment "
        "FROM annotations a WHERE a.dataset_id = ?"
    )
    annotations = (
        conn.execute(query_annotations, [dataset_id])
        .fetchdf()
        .to_dict(orient="records")
    )

    # 7. Fetch Molecules linked via annotations & their external DB IDs
    # Requete SQL Molecules + Provenance de l'annotation d'origine
    query_molecules = (
        "SELECT m.molecule_id, m.name, m.formula, m.organism, m.molecule_type_label, "
        "a.provenance_label "
        "FROM molecules m JOIN annotations a ON m.annotation_id = a.annotation_id "
        "WHERE a.dataset_id = ?"
    )
    molecules = (
        conn.execute(query_molecules, [dataset_id]).fetchdf().to_dict(orient="records")
    )

    query_mol_dbs = (
        "SELECT database_label, id_in_external_database, url_in_external_database "
        "FROM molecules_external_databases WHERE molecule_id = ?"
    )
    for mol in molecules:
        mol["external_dbs"] = (
            conn.execute(query_mol_dbs, [mol["molecule_id"]])
            .fetchdf()
            .to_dict(orient="records")
        )

    return templates.TemplateResponse(
        request=request,
        name="dataset_detail.html",
        context={
            "dataset": dataset,
            "files": files,
            "authors": authors,
            "publications": publications,
            "models": models,
            "annotations": annotations,
            "molecules": molecules,
        },
    )


@router.get("/datasets/{dataset_id}/download_json")
def download_dataset_json(dataset_id: int, conn: ConnDep):
    """Generate and trigger browser download for dataset metadata as a JSON file."""
    # 1. Main dataset details
    query_dataset = "SELECT * FROM datasets WHERE dataset_id = ?"
    dataset_res = (
        conn.execute(query_dataset, [dataset_id]).fetchdf().to_dict(orient="records")
    )

    if not dataset_res:
        raise HTTPException(status_code=404, detail=f"Dataset {dataset_id} not found.")

    dataset_data = dataset_res[0]

    # 2. Files associated + list of external URLs
    query_files = "SELECT * FROM files WHERE dataset_id = ?"
    files = conn.execute(query_files, [dataset_id]).fetchdf().to_dict(orient="records")
    file_urls = [f["url"] for f in files if f.get("url")]

    # 3. Authors
    query_authors = (
        "SELECT p.* FROM persons p JOIN datasets_authors_link dal "
        "ON p.person_id = dal.person_id WHERE dal.dataset_id = ?"
    )
    authors = (
        conn.execute(query_authors, [dataset_id]).fetchdf().to_dict(orient="records")
    )

    # 4. Publications
    query_pubs = (
        "SELECT pub.* FROM publications pub JOIN datasets_publications_link dpl "
        "ON pub.publication_id = dpl.publication_id WHERE dpl.dataset_id = ?"
    )
    publications = (
        conn.execute(query_pubs, [dataset_id]).fetchdf().to_dict(orient="records")
    )

    # 5. AI Models
    query_models = (
        "SELECT m.* FROM ai_models m JOIN datasets_models_link dml "
        "ON m.ai_model_id = dml.ai_model_id WHERE dml.dataset_id = ?"
    )
    models = (
        conn.execute(query_models, [dataset_id]).fetchdf().to_dict(orient="records")
    )

    # 6. Annotations & Molecules
    query_ann = "SELECT * FROM annotations WHERE dataset_id = ?"
    annotations = (
        conn.execute(query_ann, [dataset_id]).fetchdf().to_dict(orient="records")
    )

    query_mols = (
        "SELECT m.* FROM molecules m JOIN annotations a ON m.annotation_id = a.annotation_id "
        "WHERE a.dataset_id = ?"
    )
    molecules = (
        conn.execute(query_mols, [dataset_id]).fetchdf().to_dict(orient="records")
    )

    query_mol_dbs = (
        "SELECT database_label, id_in_external_database, url_in_external_database "
        "FROM molecules_external_databases WHERE molecule_id = ?"
    )
    for mol in molecules:
        mol["external_dbs"] = (
            conn.execute(query_mol_dbs, [mol["molecule_id"]])
            .fetchdf()
            .to_dict(orient="records")
        )

    # Construct complete JSON payload
    export_payload = {
        "dataset": dataset_data,
        "authors": authors,
        "publications": publications,
        "ai_models": models,
        "annotations": annotations,
        "molecules": molecules,
        "files_count": len(files),
        "file_urls": file_urls,
        "files": files,
    }

    # Format JSON string
    json_content = json.dumps(export_payload, indent=2, default=str)

    # Headers to trigger browser download
    headers = {
        "Content-Disposition": f"attachment; filename=mdverse_dataset_{dataset_id}.json"
    }

    return Response(
        content=json_content, media_type="application/json", headers=headers
    )
