"""Router for the tools section of the frontend."""

from fastapi import APIRouter, Request
from fastapi.responses import HTMLResponse

from ..database import templates

router = APIRouter(prefix="/tools", tags=["Tools Frontend"], include_in_schema=False)


@router.get("/rest-api", response_class=HTMLResponse)
def page_tool_rest_api(request: Request):
    return templates.TemplateResponse(request=request, name="tools/rest_api.html")


@router.get("/metamd", response_class=HTMLResponse)
def page_tool_metamd(request: Request):
    return templates.TemplateResponse(request=request, name="tools/metamd.html")


@router.get("/paper2repo", response_class=HTMLResponse)
def page_tool_paper2repo(request: Request):
    return templates.TemplateResponse(request=request, name="tools/paper2repo.html")
