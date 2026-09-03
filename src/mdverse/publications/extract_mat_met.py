"""Extract Materials and Methods sections from stored XML in Parquet files."""

import re
from pathlib import Path

import click
import loguru
import pandas as pd
from bs4 import BeautifulSoup, ParserRejectedMarkup, Tag
from lxml import etree

from mdverse.core.logger import create_logger
from mdverse.scrapers.europe_pmc import clean_markup_text

METHODS_KEYWORDS = [
    "method",
    "materials and methods",
    "experimental",
    "simulation details",
    "protocol",
    "computational methods",
    "methodology",
    "molecular dynamics",
    "MD",
]


def extract_section_payload(section_tag: Tag) -> tuple[str, str]:
    """Extract section title and joined paragraph content from a section tag.

    Parameters
    ----------
    section_tag : Tag
        BeautifulSoup XML section node (<sec>).

    Returns
    -------
    tuple[str, str]
        Tuple containing section title and normalized concatenated paragraph text.
    """
    # Locate title tag directly within section.
    title_tag = section_tag.find("title")
    # Clean title text or assign fallback label.
    title_text = title_tag.get_text(strip=True) if title_tag else ""
    # Iterate over title and paragraph child nodes.
    content_elements = []
    for element in section_tag.find_all(["title", "p"]):
        # Example input nodes: <title>2. Methods</title>, <p>Simulations in GROMACS.</p>
        extracted_text = element.get_text(strip=True)
        if extracted_text:
            # Example extracted elements: ["2. Methods", "Simulations in GROMACS."]
            content_elements.append(extracted_text)
    # Join extracted parts.
    joined_text = "\n\n".join(content_elements).strip()
    return title_text, joined_text


def get_section_text_length(candidate_entry: tuple[str, str]) -> int:
    """Return character length of candidate section text.

    Returns
    -------
    int
        Length of the section text for comparison.
    """
    _, section_text = candidate_entry
    return len(section_text)


def process_child_sections(
    section_node: Tag,
    keywords_pattern: re.Pattern,
    parent_info: tuple[str, str],
    pmcid: str,
    logger: "loguru.Logger" = loguru.logger,
    *,
    parent_matches: bool,
) -> list[tuple[str, str]]:
    """Process child <sec> nodes of a parent section, logging matches and warnings.

    Returns
    -------
    list[tuple[str, str]]
        List of matching child section titles and their text content.
    """
    matching_children = []
    for child in section_node.find_all("sec"):
        # Extract title and text for the child section.
        c_title, c_text = extract_section_payload(child)
        # Check if the child section title matches the Methods keywords.
        if c_text and keywords_pattern.search(c_title):
            matching_children.append((c_title, c_text))
    # Warn if subsections match while the parent does not
    if not parent_matches and matching_children:
        p_title, p_text = parent_info
        logger.warning(
            f"[{pmcid}] disp-level 1 non-match: '{p_title}' (length: {len(p_text)})"
        )
    for c_title, c_text in matching_children:
        logger.debug(
            f"[{pmcid}] └── disp-level 2 match: '{c_title}' (length: {len(c_text)})"
        )
    return matching_children


def extract_methods_from_xml(
    xml_content: str | None,
    pmcid: str,
    logger: "loguru.Logger" = loguru.logger,
) -> str | None:
    """Extract Materials and Methods sections from XML content.

    Returns
    -------
    str | None
        Extracted and sanitized text content, or None if matching sections are missing.
    """
    # Parse raw XML into BeautifulSoup tree.
    try:
        soup = BeautifulSoup(xml_content, "xml")
    except (ParserRejectedMarkup, etree.XMLSyntaxError, ValueError, TypeError) as error:
        logger.error(f"Failed to parse XML content for {pmcid}: {error}.")
        return None
    else:
        # Compile regex pattern for Methods keywords.
        keywords_pattern = re.compile("|".join(METHODS_KEYWORDS), re.IGNORECASE)
        # Iterate over top-level sections and inspect child subsections.
        level_1_candidates = []
        level_2_candidates = []
        for section_node in soup.find_all("sec"):
            if section_node.find_parent("sec"):
                continue
            # Check if the section matches the Methods keywords.
            title, text = extract_section_payload(section_node)
            parent_matches = bool(text and keywords_pattern.search(title))
            if parent_matches:
                logger.debug(
                    f"[{pmcid}] disp-level 1 match: '{title}' (length: {len(text)})"
                )
                level_1_candidates.append((title, text))
            # Collect matching subsections for the current section
            child_matches = process_child_sections(
                section_node,
                keywords_pattern,
                parent_info=(title, text),
                pmcid=pmcid,
                logger=logger,
                parent_matches=parent_matches,
            )
            level_2_candidates.extend(child_matches)

        # Priority selection: disp-level 1 first, then fallback to disp-level 2.
        for level_label, candidates in (
            ("disp-level 1", level_1_candidates),
            ("disp-level 2", level_2_candidates),
        ):
            if candidates:
                # Select the candidate with the longest text content.
                chosen_title, chosen_text = max(candidates, key=get_section_text_length)
                logger.info(
                    f"[{pmcid}] Extracted {level_label} section '{chosen_title}' "
                    f"(length: {len(chosen_text)})."
                )
                # Return the cleaned and normalized text content of the chosen section.
                return clean_markup_text(chosen_text)

        logger.critical(f"Neither Methods nor MD sections found in {pmcid}.")
        return None


def process_parquet_methods(
    parquet_path: Path,
    out_path: Path,
    logger: "loguru.Logger" = loguru.logger,
) -> None:
    """Process a Parquet dataset to extract Methods sections from XML content."""
    # Load the Parquet dataset into a DataFrame.
    dataset_frame = pd.read_parquet(parquet_path)
    logger.info(f"Loaded {len(dataset_frame)} publications from {parquet_path.name}.")
    # Check for the presence of the 'full_text_xml' column.
    if "full_text_xml" not in dataset_frame.columns:
        logger.error("Column 'full_text_xml' not found in the Parquet dataset.")
        return
    # Process XML payload for each entry in the DataFrame.
    methods_records = []
    for index, row in dataset_frame.iterrows():
        xml_content = row.get("full_text_xml")
        identifier = row.get("publication_id_in_source")
        methods_text = extract_methods_from_xml(xml_content, identifier, logger)
        methods_records.append(methods_text)
        logger.info(f"Extracted {index + 1}/{len(dataset_frame)} methods section.")
    # Overwrite destination Parquet file with the new extracted column.
    dataset_frame["materials_and_methods"] = methods_records
    dataset_frame.to_parquet(out_path, index=False)
    # Log the number of successfully extracted Methods sections.
    extracted_count = sum(section_text is not None for section_text in methods_records)
    logger.success(
        f"Extraction complete: {extracted_count}/{len(dataset_frame)} "
        f"({extracted_count / len(dataset_frame) * 100}%)."
    )
    logger.success(f"Saved updated dataset with extracted Methods to {out_path.name}.")


@click.command(help="Extract Materials and Methods sections from Parquet dataset.")
@click.option(
    "--parquet-path",
    type=click.Path(exists=True, file_okay=True, dir_okay=False, path_type=Path),
    required=True,
    help="Path to the Parquet dataset file.",
)
@click.option(
    "--out-path",
    type=click.Path(file_okay=True, dir_okay=False, path_type=Path),
    required=True,
    help="Path to the output Parquet file.",
)
def run_main_from_cli(parquet_path: Path, out_path: Path) -> None:
    """CLI entry point for extracting Methods sections from Parquet."""
    log_path = "logs/extract_mat_met.log"
    logger = create_logger(logpath=log_path, level="DEBUG")
    logger.info(f"Saved logs to {log_path}.")
    process_parquet_methods(parquet_path, out_path, logger=logger)


if __name__ == "__main__":
    run_main_from_cli()
