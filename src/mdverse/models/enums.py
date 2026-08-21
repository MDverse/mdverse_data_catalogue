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
    HUGGINGFACE = "huggingface"

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
            DatasetSourceName.HUGGINGFACE: "https://huggingface.co/",
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
            DatasetSourceName.HUGGINGFACE: (
                "Wolf, Thomas, et al. 'Huggingface's transformers: State-of-the-art "
                "natural language processing.' arXiv preprint arXiv:1910.03771 (2019)."
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
                "Online platform with web-based visualization capabilities and a "
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
            DatasetSourceName.HUGGINGFACE: (
                "The AI community building the future. The platform where the machine "
                "learning community collaborates on models, datasets, and applications."
            ),
        }
        return comments.get(self)


class PublicationSourceName(StrEnum):
    """Molecular dynamics publication sources."""

    EUROPE_PMC = "europe_pmc"
    HUGGINGFACE = "huggingface"
    ARXIV = "arxiv"

    @property
    def url(self) -> str | None:
        """Return the base URL for the publication source."""
        urls = {
            PublicationSourceName.EUROPE_PMC: "https://europepmc.org/",
            PublicationSourceName.HUGGINGFACE: "https://huggingface.co/",
            PublicationSourceName.ARXIV: "https://arxiv.org/",
        }
        return urls.get(self)

    @property
    def comment(self) -> str | None:
        """Return default descriptive comment for the publication source."""
        comments = {
            PublicationSourceName.EUROPE_PMC: (
                "Europe PubMed Central (Europe PMC) is a free database of life "
                "sciences and biomedical literature."
            ),
            PublicationSourceName.HUGGINGFACE: (
                "The AI community building the future. The platform where the machine "
                "learning community collaborates on models, datasets, and applications."
            ),
            PublicationSourceName.ARXIV: (
                "arXiv is a free distribution service and an open-access archive for "
                "scholarly articles in the fields of physics, mathematics, computer "
                "science, quantitative biology, quantitative finance, statistics, "
                "electrical engineering and systems science, and economics."
            ),
        }
        return comments.get(self)

    @property
    def citation(self) -> str | None:
        """Return default citation for the publication source."""
        citations = {
            PublicationSourceName.EUROPE_PMC: (
                "Europe PMC Consortium. “Europe PMC: a full-text literature database "
                "for the life sciences and platform for innovation.” Nucleic acids "
                "research vol. 43,Database issue (2015): D1042-8. "
                "doi:10.1093/nar/gku1061."
            ),
            PublicationSourceName.HUGGINGFACE: (
                "Wolf, Thomas, et al. 'Huggingface's transformers: State-of-the-art "
                "natural language processing.' arXiv preprint arXiv:1910.03771 (2019)."
            ),
            PublicationSourceName.ARXIV: (
                "Ginsparg, Paul. “ArXiv at 20.” Nature vol. 476,7359 145-7. "
                "10 Aug. 2011, doi:10.1038/476145a"
            ),
        }
        return citations.get(self)


class ExternalDatabaseName(StrEnum):
    """External database names."""

    PDB = "pdb"
    UNIPROT = "uniprot"
    CHEBI = "chebi"
    PUBCHEM = "pubchem"
    KEGG = "kegg"

    @property
    def url(self) -> str | None:
        """Return the base URL for the external database."""
        urls = {
            ExternalDatabaseName.PDB: "https://www.rcsb.org/",
            ExternalDatabaseName.UNIPROT: "https://www.uniprot.org/",
            ExternalDatabaseName.CHEBI: "https://www.ebi.ac.uk/chebi/",
            ExternalDatabaseName.PUBCHEM: "https://pubchem.ncbi.nlm.nih.gov/",
            ExternalDatabaseName.KEGG: "https://www.kegg.jp/",
        }
        return urls.get(self)

    @property
    def comment(self) -> str | None:
        """Return default descriptive comment for the external database."""
        comments = {
            ExternalDatabaseName.PDB: (
                "Protein Data Bank archive containing 3D biological macromolecular "
                "structure data."
            ),
            ExternalDatabaseName.UNIPROT: (
                "Comprehensive and freely accessible resource for protein sequence "
                "and functional information."
            ),
            ExternalDatabaseName.CHEBI: (
                "Chemical Entities of Biological Interest database focused on small "
                "chemical compounds."
            ),
            ExternalDatabaseName.PUBCHEM: (
                "Open chemistry database at the National Institutes of Health (NIH) "
                "containing chemical structures and biological activities."
            ),
            ExternalDatabaseName.KEGG: (
                "Kyoto Encyclopedia of Genes and Genomes database resource for "
                "understanding high-level functions and biological systems."
            ),
        }
        return comments.get(self)


class MoleculeType(StrEnum):
    """Common molecular types found in molecular dynamics simulations."""

    PROTEIN = "protein"
    NUCLEIC_ACID = "nucleic_acid"
    ION = "ion"
    LIPID = "lipid"
    CARBOHYDRATE = "carbohydrate"
    SOLVENT = "solvent"
    SMALL_MOLECULE = "small_molecule"
