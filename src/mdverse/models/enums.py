"""Enumerations for MDverse scrapers and models."""

from enum import StrEnum


class DataType(StrEnum):
    """Data types."""

    DATASETS = "datasets"
    FILES = "files"


class DatasetSourceName(StrEnum):
    """Molecular dynamics sources: data repositories and projects."""

    ZENODO = "zenodo"
    FIGSHARE = "figshare"
    OSF = "osf"
    NOMAD = "nomad"
    ATLAS = "atlas"
    GPCRMD = "gpcrmd"
    NMRLIPIDS = "nmrlipids"
    MDDB = "mddb"
    MDPOSIT = "mdposit"
    MDPOSIT_INRIA_NODE = "mdposit_inria_node"
    MDPOSIT_MMB_NODE = "mdposit_mmb_node"
    MDPOSIT_CINECA_NODE = "mdposit_cineca_node"

    @property
    def url(self) -> str | None:
        """Return the base URL for the repository."""
        urls = {
            DatasetSourceName.ZENODO: "https://zenodo.org/",
            DatasetSourceName.FIGSHARE: "https://figshare.com/",
            DatasetSourceName.ATLAS: "https://www.dsimb.inserm.fr/ATLAS/",
            DatasetSourceName.NOMAD: "https://nomad-lab.eu/",
            DatasetSourceName.GPCRMD: "https://www.gpcrmd.org/",
            DatasetSourceName.MDDB: "https://mddbr.eu/",
            DatasetSourceName.MDPOSIT: "https://mdposit.mddbr.eu/#/",
            DatasetSourceName.MDPOSIT_MMB_NODE: "https://irb-dev.mddbr.eu/",
            DatasetSourceName.MDPOSIT_INRIA_NODE: "https://inria.mddbr.eu/",
            DatasetSourceName.MDPOSIT_CINECA_NODE: "https://cineca.mddbr.eu/",
        }
        return urls.get(self)

    @property
    def citation(self) -> str | None:
        """Return default citation for the repository."""
        citations = {
            DatasetSourceName.ZENODO: (
                "European Organization For Nuclear Research and OpenAIRE. "
                "(2013). Zenodo. CERN. https://doi.org/10.25495/7GXK-RD71"
            ),
            DatasetSourceName.FIGSHARE: (
                "Singh, Jatinder. 'FigShare.' Journal of pharmacology & "
                "pharmacotherapeutics vol. 2,2 (2011): 138-9. "
                "doi:10.4103/0976-500X.81919"
            ),
            DatasetSourceName.ATLAS: (
                "Yann Vander Meersche, Gabriel Cretin, Aria Gheeraert, "
                "Jean-Christophe Gelly, Tatiana Galochkina, ATLAS: protein "
                "flexibility description from atomistic molecular dynamics "
                "simulations, Nucleic Acids Research, Volume 52, Issue D1, "
                "5 January 2024, Pages D384–D392, "  # noqa: RUF001
                "https://doi.org/10.1093/nar/gkad1084"
            ),
            DatasetSourceName.NOMAD: (
                "Scheidgen et al., (2023). NOMAD: A distributed web-based "
                "platform for managing materials science research data. "
                "Journal of Open Source Software, 8(90), 5388, "
                "https://doi.org/10.21105/joss.05388"
            ),
            DatasetSourceName.GPCRMD: (
                "Rodríguez-Espigares, Ismael et al. 'GPCRmd uncovers the "
                "dynamics of the 3D-GPCRome.' Nature methods vol. 17,8 "
                "(2020): 777-787. doi:10.1038/s41592-020-0884-y"
            ),
            DatasetSourceName.MDDB: (
                "Amaro, Rommie E., et al. 'The need to implement FAIR "
                "principles in biomolecular simulations.' Nature Methods, "
                "vol. 22, p. 641-645, 2025. Nature Publishing Group, 2025. "
                "https://hdl.handle.net/2445/220970"
            ),
        }
        return citations.get(self)

    @property
    def comment(self) -> str | None:
        """Return default descriptive comment for the repository."""
        comments = {
            DatasetSourceName.ZENODO: (
                "General-purpose open-access repository developed under the "
                "European OpenAIRE program."
            ),
            DatasetSourceName.FIGSHARE: (
                "Online open access repository where researchers can preserve "
                "and share their research outputs."
            ),
            DatasetSourceName.GPCRMD: (
                "online platform with web-based visualization capabilities and a "
                "comprehensive analysis toolbox that allows visualizing, inspecting, "
                "and analysing GPCR molecular dynamics."
            ),
            DatasetSourceName.NOMAD: (
                "Free web-service that lets you share your data or use comprehensive "
                "data that others provide. You can use NOMAD to organize, analyze, "
                "share, and publish your materials science data, as well as explore, "
                "download, and analyze your colleagues' data."
            ),
            DatasetSourceName.ATLAS: (
                "Database that gathers standardized molecular dynamics simulations of "
                "protein structures accompanied by their analysis in the form of "
                "interactive diagrams and trajectory visualisation. All the raw "
                "trajectories as well as the results of analysis are available for "
                "download dynamics simulations."
            ),
            DatasetSourceName.MDPOSIT: (
                "Open platform designed to provide web access to atomistic "
                "molecular dynamics (MD) simulations. The aim of this initiative "
                "is to ease and promote data sharing along the wide-world scientific "
                "community in order to contribute in research."
            ),
        }
        return comments.get(self)


class ExternalDatabaseName(StrEnum):
    """External database names."""

    PDB = "pdb"
    UNIPROT = "uniprot"
    CHEBI = "chebi"
    PUBCHEM = "pubchem"
    KEGG = "kegg"


class MoleculeType(StrEnum):
    """Common molecular types found in molecular dynamics simulations."""

    PROTEIN = "protein"
    NUCLEIC_ACID = "nucleic_acid"
    ION = "ion"
    LIPID = "lipid"
    CARBOHYDRATE = "carbohydrate"
    SOLVENT = "solvent"
    SMALL_MOLECULE = "small_molecule"
