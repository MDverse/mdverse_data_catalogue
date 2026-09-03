"""Pydantic data models used to validate scraped molecular dynamics publications."""

from pydantic import BaseModel, ConfigDict, Field

from mdverse.models.dataset import DOI
from mdverse.models.enums import PublicationSourceName
from mdverse.models.person import Person


class PublicationCoreMetadata(BaseModel):
    """Core provenance metadata shared by publication models.

    This model captures essential information about the source publication.
    """

    model_config = ConfigDict(extra="forbid")

    doi: DOI | None = Field(
        None,
        description=(
            "Digital Object Identifier (DOI) of the publication. "
            "Must start with '10.' and follow the standard DOI pattern."
        ),
    )
    publication_source_name: PublicationSourceName | None = Field(
        None,
        description=(
            "Name of the source publication. "
            "Allowed values in the PublicationSourceName enum. "
            "Examples: EUROPE_PMC, HUGGINGFACE, ARXIV..."
        ),
    )
    publication_id_in_source: str | None = Field(
        None,
        description=(
            "Unique identifier of the publication in the source platform "
            "(e.g., PMCID 'PMC1234567' or arXiv ID '2407.14794')."
        ),
    )
    url: str | None = Field(
        None, description="Direct URL to access the publication page."
    )


class PublicationMetadata(PublicationCoreMetadata):
    """Base Pydantic model for molecular dynamics publications."""

    model_config = ConfigDict(extra="forbid")

    title: str = Field(..., description="Title of the publication.")
    authors: list[Person] = Field(
        default_factory=list, description="List of authors of the publication."
    )
    year: str = Field(..., description="Year of publication in YYYY format.")
    abstract: str | None = Field(None, description="Abstract of the publication.")
    journal: str | None = Field(None, description="Journal of the publication.")
    keywords: list[str] = Field(
        default_factory=list,
        description="List of keywords associated with the publication.",
    )
    full_text_xml: str | None = Field(
        None, description="Raw JATS XML full text of the publication."
    )
    materials_and_methods: str | None = Field(
        None, description="Extracted Materials and Methods section text."
    )
    # ------------------------------------------------------------------
    # External references
    # ------------------------------------------------------------------
    external_links: list[str] = Field(
        default_factory=list,
        description="List of external links (URLs) referenced in the publication.",
    )
