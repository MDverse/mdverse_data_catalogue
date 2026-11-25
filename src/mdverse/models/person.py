"""Pydantic data models used to validate Person metadata from MD datasets."""

from pydantic import BaseModel, ConfigDict, Field, field_validator


class Person(BaseModel):
    """Person of dataset or publication."""

    # Ensure scraped metadata matches the expected schema exactly.
    model_config = ConfigDict(extra="forbid")

    first_name: str | None = Field(None, description="Person first name.")
    last_name: str | None = Field(None, description="Person last name.")
    full_name: str | None = Field(None, description="Person full name.")
    orcid: str | None = Field(None, description="Person ORCID.")
    affiliation: str | None = Field(None, description="Person affiliation.")

    # ------------------------------------------------------------------
    # Validators
    # ------------------------------------------------------------------
    @field_validator("*", mode="before")
    @classmethod
    def strip_and_nullify_empty_strings(cls, value: object) -> object:
        """Strip leading and trailing whitespace, and convert empty strings to None.

        Returns
        -------
        object
            The stripped string, None if the string is empty after stripping,
            or the unchanged value if it is not a string.
        """
        if not isinstance(value, str):
            return value
        # Remove leading and trailing whitespace.
        value = value.strip()
        # Empty strings are considered as missing values.
        return value or None
