"""Tests for the ATLAS scraper module."""

import pytest

import mdverse.scrapers.atlas as atlas_scraper
from mdverse.models.enums import ExternalDatabaseName, MoleculeType
from mdverse.models.simulation import (
    ExternalIdentifier,
    ForceFieldModel,
    Molecule,
    SimulationMetadata,
    Software,
)


@pytest.fixture
def simulation_metadata() -> SimulationMetadata:
    """Build simulation metadata similar to what the ATLAS scraper produces.

    Returns
    -------
    SimulationMetadata
        Simulation metadata for a single protein chain.
    """
    molecule = Molecule(
        name="Phosphoglycerate kinase",
        organism="Trypanosoma brucei brucei",
        type=MoleculeType.PROTEIN,
        external_identifiers=[
            ExternalIdentifier(
                database_name=ExternalDatabaseName.PDB, identifier="16pk"
            ),
            ExternalIdentifier(
                database_name=ExternalDatabaseName.UNIPROT, identifier="P07378"
            ),
        ],
    )
    return SimulationMetadata(
        molecules=[molecule],
        software=[Software(name="GROMACS", version="v2019.6")],
        forcefields_models=[
            ForceFieldModel(name="CHARMM36m", version="July 2020"),
            ForceFieldModel(name="TIP3P"),
        ],
        simulation_temperatures_in_kelvin=[300],
        simulation_times=["100 ns"],
        simulation_timesteps_in_fs=[2],
    )


def test_generate_description(simulation_metadata: SimulationMetadata) -> None:
    """Test the generate_description function with complete metadata."""
    expected_description = (
        "[automatically generated] Molecular dynamics simulation of "
        "Phosphoglycerate kinase from organism Trypanosoma brucei brucei "
        "(pdb ID: 16pk, uniprot ID: P07378) "
        "with GROMACS v2019.6 "
        "using CHARMM36m July 2020 and TIP3P "
        "at 300.0 K "
        "for 100 ns."
    )
    description = atlas_scraper.generate_description(simulation_metadata)
    assert description == expected_description


def test_generate_description_without_external_identifiers(
    simulation_metadata: SimulationMetadata,
) -> None:
    """Test that no parentheses are added when no external identifier is found."""
    simulation_metadata.molecules[0].external_identifiers = []
    description = atlas_scraper.generate_description(simulation_metadata)
    assert "ID:" not in description
    assert "()" not in description
    assert (
        "Phosphoglycerate kinase from organism Trypanosoma brucei brucei "
        "with GROMACS v2019.6 "
    ) in description


def test_generate_description_without_organism(
    simulation_metadata: SimulationMetadata,
) -> None:
    """Test that the 'from organism' clause is skipped when no organism is given."""
    simulation_metadata.molecules[0].organism = None
    description = atlas_scraper.generate_description(simulation_metadata)
    assert "from organism" not in description
    assert (
        "Phosphoglycerate kinase (pdb ID: 16pk, uniprot ID: P07378) with GROMACS"
    ) in description


def test_generate_description_without_forcefields_models(
    simulation_metadata: SimulationMetadata,
) -> None:
    """Test that the 'using' clause is skipped when no forcefield is given."""
    simulation_metadata.forcefields_models = []
    description = atlas_scraper.generate_description(simulation_metadata)
    assert "using" not in description
    assert "with GROMACS v2019.6 at 300.0 K for 100 ns." in description
