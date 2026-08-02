"""Pydantic data models used to validate scraped molecular dynamics publication."""

from pydantic import BaseModel, ConfigDict, Field

from mdverse.models.dataset import DOI
from mdverse.models.person import Person
from mdverse.models.simulation import SimulationMetadata


class PublicationMetadata(BaseModel):
    """Base Pydantic model for molecular dynamics publications."""

    # Ensure scraped metadata matches the expected schema exactly.
    model_config = ConfigDict(extra="forbid")

    doi: DOI | None = Field(
        None, description="Digital Object Identifier of the publication."
    )
    title: str = Field(..., description="Title of the publication.")
    authors: list[Person] = Field(
        default_factory=list, description="List of authors of the publication."
    )
    year: str = Field(..., description="Year of publication in YYYY format.")
    url: str | None = Field(None, description="URL to access the publication.")
    abstract: str | None = Field(None, description="Abstract of the publication.")
    keywords: list[str] = Field(
        default_factory=list,
        description="List of keywords associated with the publication.",
    )
    simulation: SimulationMetadata | None = Field(
        None,
        description="Simulation metadata associated with the publication.",
    )
