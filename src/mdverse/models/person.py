"""Pydantic data models used to validate Author metadata from MD datasets."""

from pydantic import BaseModel, ConfigDict, Field


class Person(BaseModel):
    """Author of dataset or publication."""

    # Ensure scraped metadata matches the expected schema exactly.
    model_config = ConfigDict(extra="forbid")

    first_name: str | None = Field(None, description="Author first name.")
    last_name: str | None = Field(None, description="Author last name.")
    full_name: str | None = Field(None, description="Author full name.")
    orcid: str | None = Field(None, description="Author ORCID.")
    affiliation: str | None = Field(None, description="Author affiliation.")
