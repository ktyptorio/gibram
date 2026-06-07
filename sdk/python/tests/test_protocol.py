from gibram._protocol import _Protocol
from gibram.proto import gibram_pb2 as pb


def test_decode_info_response_maps_durability_metadata():
    payload = pb.InfoResponse(
        version="0.3.0",
        document_count=1,
        textunit_count=2,
        entity_count=3,
        relationship_count=4,
        community_count=5,
        vector_dim=1536,
        session_count=6,
        session_store_mode="durable",
        wal_sync_policy="periodic",
        wal_sync_interval_ms=250,
    ).SerializeToString()

    result = _Protocol.decode_info_response(payload)

    assert result["version"] == "0.3.0"
    assert result["session_store_mode"] == "durable"
    assert result["wal_sync_policy"] == "periodic"
    assert result["wal_sync_interval_ms"] == 250


def test_decode_query_response_maps_retrieval_stats():
    payload = pb.QueryResponse(
        query_id=42,
        stats=pb.QueryStats(
            duration_micros=1500,
            vector_searches=1,
            graph_traversals=2,
            skipped_seed_indexes=["textunit", "community"],
        ),
    ).SerializeToString()

    result = _Protocol.decode_query_response(payload)

    assert result["query_id"] == 42
    assert result["execution_time_ms"] == 1.5
    assert result["vector_searches"] == 1
    assert result["graph_traversals"] == 2
    assert result["skipped_seed_indexes"] == ["textunit", "community"]
