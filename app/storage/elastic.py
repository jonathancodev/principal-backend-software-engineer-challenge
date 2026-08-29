"""Elasticsearch integration: full-text search over event metadata.

Mapping rationale (see ARCHITECTURE.md for the longer version):

- ``event_type`` / ``user_id`` / ``event_id`` are ``keyword``: exact-match
  filters and aggregations, never free-text relevance.
- ``timestamp`` is ``date`` for range filters.
- ``source_url`` is ``keyword`` with a ``text`` subfield: exact filtering by
  default, tokenized matching ("checkout", "pricing") when searching.
- ``metadata`` is ``flattened``: metadata is client-controlled and unbounded,
  so per-key dynamic mappings would explode the field count (mapping
  explosion). ``flattened`` keeps every key filterable at keyword precision.
- ``metadata_text`` is an analyzed ``text`` catch-all built at index time from
  all metadata string values (``flattened`` fields don't support full-text
  scoring), using the standard analyzer — good tokenization for mixed
  browser/device/product strings without language assumptions.
"""

import logging
from datetime import datetime
from typing import Any, Dict, List, Optional

from elasticsearch import AsyncElasticsearch

from app.domain.models import EventRecord
from app.errors import SearchUnavailableError

logger = logging.getLogger(__name__)

EVENT_INDEX_BODY: Dict[str, Any] = {
    "settings": {
        "number_of_shards": 1,
        "number_of_replicas": 0,  # single-node assessment setup; >=1 in production
        "refresh_interval": "1s",
    },
    "mappings": {
        "properties": {
            "event_id": {"type": "keyword"},
            "event_type": {"type": "keyword"},
            "user_id": {"type": "keyword"},
            "timestamp": {"type": "date"},
            "ingested_at": {"type": "date"},
            "source_url": {
                "type": "keyword",
                "fields": {"text": {"type": "text", "analyzer": "standard"}},
            },
            "metadata": {"type": "flattened"},
            "metadata_text": {"type": "text", "analyzer": "standard"},
        }
    },
}


def metadata_to_text(value: Any) -> str:
    """Flatten metadata values into a searchable text blob."""
    parts: List[str] = []

    def _walk(node: Any) -> None:
        if isinstance(node, dict):
            for key, child in node.items():
                parts.append(str(key))
                _walk(child)
        elif isinstance(node, (list, tuple)):
            for child in node:
                _walk(child)
        elif node is not None:
            parts.append(str(node))

    _walk(value)
    return " ".join(parts)


class ElasticEventStore:
    def __init__(self, client: AsyncElasticsearch, index: str) -> None:
        self._client = client
        self._index = index

    async def ensure_index(self) -> None:
        exists = await self._client.indices.exists(index=self._index)
        if not exists:
            await self._client.indices.create(index=self._index, body=EVENT_INDEX_BODY)
            logger.info("created elasticsearch index name=%s", self._index)

    async def ping(self) -> bool:
        return await self._client.ping()

    async def index_event(self, record: EventRecord) -> None:
        """Index one event. Callers treat failures as non-fatal (best-effort)."""
        doc = record.model_dump(mode="json")
        doc["metadata_text"] = metadata_to_text(record.metadata)
        await self._client.index(index=self._index, id=record.event_id, document=doc)

    async def search(
        self,
        query_text: str,
        event_type: Optional[str] = None,
        user_id: Optional[str] = None,
        start: Optional[datetime] = None,
        end: Optional[datetime] = None,
        limit: int = 20,
    ) -> Dict[str, Any]:
        filters: List[Dict[str, Any]] = []
        if event_type:
            filters.append({"term": {"event_type": event_type}})
        if user_id:
            filters.append({"term": {"user_id": user_id}})
        time_range: Dict[str, Any] = {}
        if start:
            time_range["gte"] = start.isoformat()
        if end:
            time_range["lt"] = end.isoformat()
        if time_range:
            filters.append({"range": {"timestamp": time_range}})

        body = {
            "query": {
                "bool": {
                    "must": [
                        {
                            "multi_match": {
                                "query": query_text,
                                "fields": ["metadata_text", "source_url.text"],
                                "operator": "and",
                            }
                        }
                    ],
                    "filter": filters,
                }
            },
            "size": limit,
            "sort": [{"_score": "desc"}, {"timestamp": "desc"}],
        }
        try:
            response = await self._client.search(index=self._index, body=body)
        except Exception as exc:  # transport, connection and API errors alike
            raise SearchUnavailableError(str(exc)) from exc

        hits = response["hits"]
        results = []
        for hit in hits["hits"]:
            doc = hit["_source"]
            doc.pop("metadata_text", None)  # internal field, not part of the contract
            results.append({"score": hit["_score"], "event": doc})
        return {"total": hits["total"]["value"], "results": results}

    async def close(self) -> None:
        await self._client.close()
