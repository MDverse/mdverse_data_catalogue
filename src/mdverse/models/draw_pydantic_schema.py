"""Script to generate an Entity-Relationship Diagram from Pydantic models."""

from pathlib import Path

import click
import erdantic as erd

from mdverse.core.logger import create_logger
from mdverse.models.dataset import DatasetMetadata
from mdverse.models.file import FileMetadata
from mdverse.models.publication import PublicationMetadata
from mdverse.models.simulation import SimulationMetadata


@click.command()
@click.option(
    "--out-path",
    type=click.Path(writable=True, path_type=Path),
    help="Path where the generated data model image will be saved.",
)
def main(out_path: Path) -> None:
    """Generate and save an Entity-Relationship Diagram for Pydantic metadata models."""
    logger = create_logger()
    logger.info("Building schema for data model...")
    diagram = erd.EntityRelationshipDiagram()
    # Add Pydantic models to the diagram
    diagram.add_model(DatasetMetadata)
    diagram.add_model(SimulationMetadata)
    diagram.add_model(FileMetadata)
    diagram.add_model(PublicationMetadata)
    # Ensure parent directory exists before saving
    out_path.parent.mkdir(parents=True, exist_ok=True)
    diagram.draw(out_path)
    logger.success(f"Diagram successfully saved to: {out_path}")


if __name__ == "__main__":
    main()
