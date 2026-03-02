from __future__ import annotations

import logging

from azure.core.credentials import AzureKeyCredential
from azure.core.exceptions import ResourceNotFoundError
from azure.search.documents.indexes import SearchIndexClient, SearchIndexerClient
from azure.search.documents.indexes.models import (
    AzureOpenAIVectorizer,
    AzureOpenAIVectorizerParameters,
    HnswAlgorithmConfiguration,
    FieldMapping,
    InputFieldMappingEntry,
    OutputFieldMappingEntry,
    SearchField,
    SearchFieldDataType,
    SearchIndex,
    SearchIndexer,
    SearchIndexerDataContainer,
    SearchIndexerDataSourceConnection,
    SearchIndexerDataSourceType,
    IndexingParameters,
    SearchIndexerSkillset,
    SearchableField,
    SemanticConfiguration,
    SemanticField,
    SemanticPrioritizedFields,
    SemanticSearch,
    SimpleField,
    SoftDeleteColumnDeletionDetectionPolicy,
    VectorSearch,
    VectorSearchProfile,
    AzureOpenAIEmbeddingSkill,
)

from src.config import settings

logger = logging.getLogger("uvicorn.error")


# Build an index management client.
def _index_client() -> SearchIndexClient:
    return SearchIndexClient(settings.search_endpoint, AzureKeyCredential(settings.search_admin_key))


# Build an indexer management client.
def _indexer_client() -> SearchIndexerClient:
    return SearchIndexerClient(settings.search_endpoint, AzureKeyCredential(settings.search_admin_key))


# Validate embedding model name and dimension alignment early.
def _validate_embedding_config() -> None:
    # Keep vectorizer and embedding skill on the same model+dimension to avoid runtime mismatch.
    recommended_dims = {
        "text-embedding-3-small": 1536,
        "text-embedding-3-large": 3072,
        "text-embedding-ada-002": 1536,
    }
    recommended = recommended_dims.get(settings.aoai_embedding_model_name)
    if recommended and settings.aoai_embedding_dimensions != recommended:
        raise ValueError(
            f"Embedding dimension mismatch: model={settings.aoai_embedding_model_name} "
            f"expects {recommended}, got {settings.aoai_embedding_dimensions}."
        )


# Check whether an Azure Search resource exists.
def _exists(getter, name: str) -> bool:
    try:
        getter(name)
        return True
    except ResourceNotFoundError:
        return False


# Define search index schema, vector config, and semantic config.
def build_index() -> SearchIndex:
    return SearchIndex(
        name=settings.search_index_name,
        fields=[
            SimpleField(name="chunk_id", type=SearchFieldDataType.String, key=True, filterable=True),
            SimpleField(name="video_id", type=SearchFieldDataType.String, filterable=True, sortable=True),
            SimpleField(name="start_ms", type=SearchFieldDataType.Int64, filterable=True, sortable=True),
            SimpleField(name="end_ms", type=SearchFieldDataType.Int64, filterable=True, sortable=True),
            SearchableField(name="content", type=SearchFieldDataType.String, analyzer_name="standard.lucene"),
            SearchField(
                name="content_vector",
                type=SearchFieldDataType.Collection(SearchFieldDataType.Single),
                searchable=True,
                vector_search_dimensions=settings.aoai_embedding_dimensions,
                vector_search_profile_name="vector-profile-default",
            ),
            SimpleField(name="chunk_language", type=SearchFieldDataType.String, filterable=True),
            SimpleField(name="dominant_language", type=SearchFieldDataType.String, filterable=True),
            SimpleField(name="source_path", type=SearchFieldDataType.String, filterable=True),
            SimpleField(name="video_url", type=SearchFieldDataType.String),
            SearchableField(name="metadata_json", type=SearchFieldDataType.String),
        ],
        vector_search=VectorSearch(
            algorithms=[HnswAlgorithmConfiguration(name="hnsw-default")],
            profiles=[
                VectorSearchProfile(
                    name="vector-profile-default",
                    algorithm_configuration_name="hnsw-default",
                    vectorizer_name="aoai-vectorizer",
                )
            ],
            vectorizers=[
                AzureOpenAIVectorizer(
                    vectorizer_name="aoai-vectorizer",
                    parameters=AzureOpenAIVectorizerParameters(
                        resource_url=settings.aoai_endpoint,
                        deployment_name=settings.aoai_embedding_deployment,
                        model_name=settings.aoai_embedding_model_name,
                        api_key=settings.aoai_api_key,
                    ),
                )
            ],
        ),
        semantic_search=SemanticSearch(
            default_configuration_name="sem-default",
            configurations=[
                SemanticConfiguration(
                    name="sem-default",
                    prioritized_fields=SemanticPrioritizedFields(
                        content_fields=[SemanticField(field_name="content")],
                    ),
                )
            ],
        ),
    )


# Define blob data source for JSONL chunk documents.
def build_data_source() -> SearchIndexerDataSourceConnection:
    deletion_policy = None
    if settings.soft_delete_column_name:
        deletion_policy = SoftDeleteColumnDeletionDetectionPolicy(
            soft_delete_column_name=settings.soft_delete_column_name,
            soft_delete_marker_value=settings.soft_delete_marker_value,
        )
    return SearchIndexerDataSourceConnection(
        name=settings.search_datasource_name,
        type=SearchIndexerDataSourceType.AZURE_BLOB,
        connection_string=settings.storage_connection_string,
        container=SearchIndexerDataContainer(name=settings.search_docs_container),
        data_deletion_detection_policy=deletion_policy,
    )


# Define skillset that generates embeddings for chunk content.
def build_skillset() -> SearchIndexerSkillset:
    # JSONL 已是分段文档，这里只做 embedding。
    return SearchIndexerSkillset(
        name=settings.search_skillset_name,
        skills=[
            AzureOpenAIEmbeddingSkill(
                name="embed-content",
                context="/document",
                resource_url=settings.aoai_endpoint,
                deployment_name=settings.aoai_embedding_deployment,
                model_name=settings.aoai_embedding_model_name,
                dimensions=settings.aoai_embedding_dimensions,
                api_key=settings.aoai_api_key,
                inputs=[InputFieldMappingEntry(name="text", source="/document/content")],
                outputs=[OutputFieldMappingEntry(name="embedding", target_name="contentVectorGenerated")],
            )
        ],
    )


# Define indexer that maps JSONL fields and embedding outputs.
def build_indexer() -> SearchIndexer:
    return SearchIndexer(
        name=settings.search_indexer_name,
        data_source_name=settings.search_datasource_name,
        target_index_name=settings.search_index_name,
        skillset_name=settings.search_skillset_name,
        parameters=IndexingParameters(
            # Use raw configuration map to avoid SDK defaults (for example queryTimeout)
            # that are not supported by Azure Blob data source.
            configuration={
                "parsingMode": "jsonLines",
                "failOnUnsupportedContentType": False,
            }
        ),
        field_mappings=[
            FieldMapping(source_field_name="chunk_id", target_field_name="chunk_id"),
            FieldMapping(source_field_name="video_id", target_field_name="video_id"),
            FieldMapping(source_field_name="start_ms", target_field_name="start_ms"),
            FieldMapping(source_field_name="end_ms", target_field_name="end_ms"),
            FieldMapping(source_field_name="content", target_field_name="content"),
            FieldMapping(source_field_name="chunk_language", target_field_name="chunk_language"),
            FieldMapping(source_field_name="dominant_language", target_field_name="dominant_language"),
            FieldMapping(source_field_name="source_path", target_field_name="source_path"),
            FieldMapping(source_field_name="video_url", target_field_name="video_url"),
            FieldMapping(source_field_name="metadata_json", target_field_name="metadata_json"),
        ],
        output_field_mappings=[
            FieldMapping(
                source_field_name="/document/contentVectorGenerated",
                target_field_name="content_vector",
            )
        ],
    )


# Create or update all search resources required by this application.
def provision_search_resources() -> dict[str, str]:
    _validate_embedding_config()
    idx_client = _index_client()
    ixr_client = _indexer_client()
    result: dict[str, str] = {}

    index_exists = _exists(idx_client.get_index, settings.search_index_name)
    logger.info(
        "[Provision][Search] index=%s action=%s",
        settings.search_index_name,
        "updating(existing)" if index_exists else "creating",
    )
    idx_client.create_or_update_index(build_index())
    result["index"] = "updated(existing)" if index_exists else "created"

    ds_exists = _exists(ixr_client.get_data_source_connection, settings.search_datasource_name)
    logger.info(
        "[Provision][Search] datasource=%s action=%s",
        settings.search_datasource_name,
        "updating(existing)" if ds_exists else "creating",
    )
    ixr_client.create_or_update_data_source_connection(build_data_source())
    result["datasource"] = "updated(existing)" if ds_exists else "created"

    skillset_exists = _exists(ixr_client.get_skillset, settings.search_skillset_name)
    logger.info(
        "[Provision][Search] skillset=%s action=%s",
        settings.search_skillset_name,
        "updating(existing)" if skillset_exists else "creating",
    )
    ixr_client.create_or_update_skillset(build_skillset())
    result["skillset"] = "updated(existing)" if skillset_exists else "created"

    indexer_exists = _exists(ixr_client.get_indexer, settings.search_indexer_name)
    logger.info(
        "[Provision][Search] indexer=%s action=%s",
        settings.search_indexer_name,
        "updating(existing)" if indexer_exists else "creating",
    )
    ixr_client.create_or_update_indexer(build_indexer())
    result["indexer"] = "updated(existing)" if indexer_exists else "created"

    return result


# Trigger one indexer run asynchronously.
def run_indexer() -> None:
    _indexer_client().run_indexer(settings.search_indexer_name)


# Recreate the index and re-provision dependent resources.
def rebuild_search_resources() -> dict[str, str]:
    idx_client = _index_client()
    index_existed = _exists(idx_client.get_index, settings.search_index_name)
    if index_existed:
        logger.info("[Provision][Search] index=%s action=deleting(for rebuild)", settings.search_index_name)
        idx_client.delete_index(settings.search_index_name)
    else:
        logger.info("[Provision][Search] index=%s action=skip-delete(not found)", settings.search_index_name)

    provision_details = provision_search_resources()
    provision_details["index_rebuild"] = "deleted+recreated" if index_existed else "created"
    return provision_details
