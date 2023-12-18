from flask import request
from src.milvus import MilvusClient
from src.utils import generate_embedding_of_model
from .server import app
from src.database import CollectionTable, FileProcessProgressTable
from vines_worker_sdk.server.exceptions import ServerException, ClientException
import threading
import uuid


@app.post("/api/vector/collections/<string:name>/records")
def save_vector(name):
    team_id = request.team_id
    user_id = request.user_id

    collection = CollectionTable.find_by_name(team_id, name)
    embedding_model = collection["embeddingModel"]

    data = request.json
    text = data.get("text")
    file_url = data.get("fileURL")
    is_async = data.get("async", True)
    chunk_size = data.get("chunk_size", 2048)
    metadata = data.get("metadata", {})
    metadata["userId"] = user_id

    milvus_client = MilvusClient(collection_name=name)
    if text:
        embedding = generate_embedding_of_model(embedding_model, [text])
        res = milvus_client.insert_vectors([text], embedding, [metadata])
        CollectionTable.add_metadata_fields_if_not_exists(
            team_id, name, metadata.keys()
        )
        return {
            "insert_count": res.insert_count,
            "delete_count": res.delete_count,
            "upsert_count": res.upsert_count,
            "success_count": res.succ_count,
            "err_count": res.err_count,
        }
    elif file_url:
        task_id = str(uuid.uuid4())

        FileProcessProgressTable.create_task(
            team_id=team_id, collection_name=name, task_id=task_id
        )

        def import_document_handler():
            try:
                res = milvus_client.insert_vector_from_file(
                    embedding_model, file_url, metadata, task_id, chunk_size
                )
                CollectionTable.add_metadata_fields_if_not_exists(
                    team_id, name, metadata.keys()
                )
                return res
            except Exception as e:
                FileProcessProgressTable.mark_task_failed(
                    task_id=task_id, message=str(e)
                )

        if is_async:
            thread = threading.Thread(target=import_document_handler)
            thread.start()
            return {"taskId": task_id}
        else:
            res = import_document_handler()
            if res:
                return {
                    "insert_count": res.insert_count,
                    "delete_count": res.delete_count,
                    "upsert_count": res.upsert_count,
                    "success_count": res.succ_count,
                    "err_count": res.err_count,
                }
            else:
                return {
                    "success": False
                }
    else:
        raise ServerException("非法的请求参数，请传入 text 或者 fileUrl")


@app.post("/api/vector/collections/<string:name>/records/upsert")
def upsert_vector_batch(name):
    collection = CollectionTable.find_by_name_without_team(name)
    if not collection:
        raise ClientException(f"向量数据库 {name} 不存在")
    embedding_model = collection.get("embeddingModel")
    milvus_client = MilvusClient(collection_name=name)
    list = request.json
    pks = [item["pk"] for item in list]
    texts = [item["text"] for item in list]
    metadatas = [item["metadata"] for item in list]
    embeddings = generate_embedding_of_model(embedding_model, texts)
    result = milvus_client.upsert_record_batch(pks, texts, embeddings, metadatas)
    return {"upsert_count": result.upsert_count}


@app.post("/api/vector/collections/<string:name>/query")
def query_vector(name):
    data = request.json
    expr = data.get("expr", "")
    milvus_client = MilvusClient(collection_name=name)
    offset = data.get("offset", 0)
    limit = data.get("limit", 30)
    records = milvus_client.query_vector(
        expr=expr,
        offset=offset,
        limit=limit,
    )
    return {"records": records}


@app.post("/api/vector/collections/<string:name>/search")
def search_vector(name):
    team_id = request.team_id
    data = request.json
    collection = CollectionTable.find_by_name(team_id, name)
    embedding_model = collection["embeddingModel"]
    expr = data.get("expr")
    q = data.get("q")
    limit = data.get("limit", 30)
    embedding = generate_embedding_of_model(embedding_model, q)
    milvus_client = MilvusClient(collection_name=name)
    data = milvus_client.search_vector(embedding, expr, limit)
    return {
        "records": data,
    }


@app.delete("/api/vector/collections/<string:name>/records/<string:pk>")
def delete_record(name, pk):
    milvus_client = MilvusClient(collection_name=name)
    result = milvus_client.delete_record(pk)
    return {"delete_count": result.delete_count}


@app.put("/api/vector/collections/<string:name>/records/<string:pk>")
def upsert_record(name, pk):
    data = request.json
    team_id = request.team_id
    text = data.get("text")
    metadata = data.get("metadata")
    collection = CollectionTable.find_by_name(team_id, name)
    embedding_model = collection["embeddingModel"]
    embedding = generate_embedding_of_model(embedding_model, [text])
    milvus_client = MilvusClient(collection_name=name)
    result = milvus_client.upsert_record(pk, text, embedding, metadata)
    return {"upsert_count": result.upsert_count}
