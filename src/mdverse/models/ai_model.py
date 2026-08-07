"""Pydantic data models used to validate machine learning and AI model metadata."""

from datetime import datetime

from pydantic import BaseModel, ConfigDict, Field, field_validator

from mdverse.models.dataset import DOI
from mdverse.models.date import DATETIME_FORMAT
from mdverse.models.enums import DatasetSourceName
from mdverse.models.person import Person


class AiModelCoreMetadata(BaseModel):
    """Core provenance metadata for AI models.

    This model captures essential information about the source
    repository and unique identifiers for AI/ML models.
    """

    model_config = ConfigDict(extra="forbid")

    repository_name: DatasetSourceName = Field(
        ...,
        description=(
            "Name of the source model repository. Examples: HUGGINGFACE, GITHUB..."
        ),
    )
    model_id_in_repository: str = Field(
        ...,
        min_length=1,
        description=(
            "Unique identifier of the model in the repository. "
            "Example: 'facebook/esm2_t33_650M_UR50D'."
        ),
    )
    model_url: str = Field(
        ...,
        description="URL to access the model repository page.",
    )


class AiModelMetadata(AiModelCoreMetadata):
    """Base Pydantic model for AI models used in Molecular Dynamics.

    This model extends DatasetCoreMetadata with model-specific metadata.
    """

    model_config = ConfigDict(extra="forbid")

    # ------------------------------------------------------------------
    # Descriptive metadata
    # ------------------------------------------------------------------
    authors: list[Person] = Field(
        default_factory=list,
        description="List of authors or creators of the model.",
    )
    description: str | None = Field(
        None,
        description="Description or summary of the model.",
    )
    license: str | None = Field(
        None,
        description="License under which the model weights are distributed.",
    )
    doi: DOI | None = Field(
        None,
        description="Digital Object Identifier (DOI) assigned to the model.",
    )
    keywords: list[str] = Field(
        default_factory=list,
        description="List of keywords describing the model.",
    )

    # ------------------------------------------------------------------
    # Technical metadata
    # ------------------------------------------------------------------
    tasks: str | None = Field(
        None,
        description="List of tasks the model is designed for,"
        "e.g., 'text-classification', 'image-generation'.",
    )
    number_of_parameters: int | None = Field(
        None,
        ge=0,
        description="Total number of parameters in the model.",
    )

    # ------------------------------------------------------------------
    # Temporal metadata
    # ------------------------------------------------------------------
    date_created: str | None = Field(
        None,
        description="Date when the model was originally created or published.",
    )
    date_last_updated: str | None = Field(
        None,
        description="Date when the model weights or card were last updated.",
    )
    date_last_fetched: str = Field(
        default_factory=lambda: datetime.now().strftime(DATETIME_FORMAT),
        description="Date when the model metadata was fetched by the scraper.",
    )

    # ------------------------------------------------------------------
    # Validators
    # ------------------------------------------------------------------
    @field_validator("date_created", "date_last_updated", mode="before")
    @classmethod
    def format_dates(cls, value: datetime | str | None) -> str | None:
        """Convert datetime objects or ISO strings to uniform DATETIME_FORMAT format.

        Returns
        -------
            str | None: The date formatted string or None.
        """
        if value is None:
            return None
        if isinstance(value, datetime):
            return value.strftime(DATETIME_FORMAT)
        return datetime.fromisoformat(value).strftime(DATETIME_FORMAT)
