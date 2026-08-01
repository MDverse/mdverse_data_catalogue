"""Helper module to enrich PublicationMetadata records."""

import httpx
import loguru
from pydantic import ValidationError

from mdverse.models.ai_model import AiModelMetadata
from mdverse.models.dataset import DatasetMetadata
from mdverse.models.enums import DatasetSourceName
from mdverse.models.publication import PublicationMetadata

HF_BASE_URL = "https://huggingface.co"


def resolve_hf_model(
    client: httpx.Client, model_id: str, logger: "loguru.Logger"
) -> AiModelMetadata | None:
    """Fetch full metadata for a Hugging Face AI Model.

    Returns
    -------
    AiModelMetadata | None:
        AiModelMetadata instance if the model is found and metadata
        is successfully retrieved, otherwise None.
    """
    url = f"{HF_BASE_URL}/api/models/{model_id}"
    try:
        resp = client.get(url)
        if resp.status_code != 200:
            return None
        data = resp.json()
        # Extract tags
        raw_tags = data.get("tags")
        keywords_list = [str(t) for t in raw_tags] if isinstance(raw_tags, list) else []
        # Safely parse parameter count if available
        safetensors_info = data.get("safetensors", {})
        param_count = (
            safetensors_info.get("total")
            if isinstance(safetensors_info, dict)
            else None
        )
        return AiModelMetadata(
            repository_name=DatasetSourceName.HUGGINGFACE,
            model_id_in_repository=model_id,
            model_url=f"https://huggingface.co/{model_id}",
            description=data.get("description"),
            license=data.get("license"),
            tasks=data.get("pipeline_tag"),
            number_of_parameters_=param_count,
            keywords=keywords_list,
        )
    except (httpx.HTTPError, ValueError, ValidationError) as err:
        logger.error(f"Failed resolving HF model {model_id}: {err}")
        return None


def resolve_hf_dataset(
    client: httpx.Client, dataset_id: str, logger: "loguru.Logger"
) -> DatasetMetadata | None:
    """Fetch full metadata for a Hugging Face Dataset.

    Returns
    -------
    DatasetMetadata | None:
        DatasetMetadata instance if the dataset is found and metadata
        is successfully retrieved, otherwise None.
    """
    url = f"{HF_BASE_URL}/api/datasets/{dataset_id}"
    try:
        resp = client.get(url)
        if resp.status_code != 200:
            return None
        data = resp.json()
        # Extract tags
        raw_tags = data.get("tags")
        keywords_list = [str(t) for t in raw_tags] if isinstance(raw_tags, list) else []
        return DatasetMetadata(
            dataset_repository_name=DatasetSourceName.HUGGINGFACE,
            dataset_id_in_repository=dataset_id,
            dataset_url_in_repository=f"https://huggingface.co/datasets/{dataset_id}",
            title=data.get("id") or dataset_id,
            description=data.get("description"),
            license=data.get("license"),
            download_number=data.get("downloads"),
            view_number=data.get("likes"),
            keywords=keywords_list,
        )
    except (httpx.HTTPError, ValueError, ValidationError) as err:
        logger.error(f"Failed resolving HF dataset {dataset_id}: {err}")
        return None


def resolve_external_dataset(
    repository_name: DatasetSourceName, dataset_id: str, logger: "loguru.Logger"
) -> DatasetMetadata | None:  # TODO: Implement actual resolution for external datasets
    """Resolve metadata for external datasets (e.g. Zenodo, Figshare, OSF, PDB).

    Returns
    -------
    DatasetMetadata | None:
        DatasetMetadata instance with minimal information
        if the repository is recognized, otherwise None.
    """


def enrich_paper_record(
    client: httpx.Client, paper: PublicationMetadata, logger: "loguru.Logger"
) -> PublicationMetadata:
    """Enrich a single PublicationMetadata instance.

    Returns
    -------
        PublicationMetadata: The updated paper record
        with resolved model and dataset references.
    """
    # Resolve Model References
    resolved_models = []
    for m_ref in paper.model_references:
        repo_enum = m_ref.repository_name
        m_id = m_ref.model_id_in_repository
        if repo_enum == DatasetSourceName.HUGGINGFACE:
            meta = resolve_hf_model(client, m_id, logger)
        else:
            logger.debug(
                f"[Placeholder] External model resolution for {repo_enum}:{m_id}"
            )
            meta = None

        if meta:
            resolved_models.append(meta)
    # Resolve Dataset References
    resolved_datasets = []
    for ds_ref in paper.dataset_references:
        repo_enum = ds_ref.dataset_repository_name
        ds_id = ds_ref.dataset_id_in_repository
        # Resolve based on repository type
        if repo_enum == DatasetSourceName.HUGGINGFACE:
            meta = resolve_hf_dataset(client, ds_id, logger)
        else:
            # Placeholder for external dataset resolution
            meta = resolve_external_dataset(repo_enum, ds_id, logger)
        if meta:
            resolved_datasets.append(meta)

    return paper
