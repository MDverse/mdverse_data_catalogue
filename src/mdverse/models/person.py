"""Pydantic data models used to validate Person metadata from MD datasets."""

from pydantic import BaseModel, ConfigDict, Field


class Person(BaseModel):
    """Person of dataset or publication."""

    # Ensure scraped metadata matches the expected schema exactly.
    model_config = ConfigDict(extra="forbid")

    first_name: str | None = Field(None, description="Person first name.")
    last_name: str | None = Field(None, description="Person last name.")
    full_name: str | None = Field(None, description="Person full name.")
    orcid: str | None = Field(None, description="Person ORCID.")
    affiliation: str | None = Field(None, description="Person affiliation.")
