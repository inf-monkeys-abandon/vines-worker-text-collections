from vines_worker_sdk.server import create_server
from flask import request
import os
from src.milvus import MilvusClient, create_milvus_collection, create_user, MILVUS_PUBLIC_ADDRESS
from src.utils import generate_embedding_of_model

SERVICE_AUTHENTICATION_TOKEN = os.environ.get("SERVICE_AUTHENTICATION_TOKEN")
if not SERVICE_AUTHENTICATION_TOKEN:
    raise Exception("请在环境变量中配置 SERVICE_AUTHENTICATION_TOKEN")

app = create_server(
    service_token=SERVICE_AUTHENTICATION_TOKEN,
    import_name="vines-worker-milvus",
)


@app.get("/api/system-info")
def get_system_info():
    [host, port] = MILVUS_PUBLIC_ADDRESS.split(":")
    return {
        "host": host,
        "port": port
    }


@app.post("/api/create-user")
def create_user_handler():
    data = request.json
    role_name = data.get('role_name')
    username = data.get('username')
    password = data.get('password')
    result = create_user(
        role_name,
        username,
        password
    )
    print(result)
    return {
        "success": True
    }


@app.post("/api/vector/save-vector-from-text")
def save_vector_from_text():
    data = request.json
    name = data.get('name')
    embedding_model = data.get('embedding_model')
    text = data.get('text')
    metadata = data.get('metadata', {})
    embedding = generate_embedding_of_model(embedding_model, [text])
    milvus_client = MilvusClient(
        collection_name=name
    )
    res = milvus_client.insert_vectors([text], embedding, [metadata])
    print(res)
    return {
        "insert_count": res.insert_count,
        "delete_count": res.delete_count,
        "upsert_count": res.upsert_count,
        "success_count": res.succ_count,
        "err_count": res.err_count
    }


@app.post("/api/vector/upload-document")
def save_vector_from_file():
    data = request.json
    name = data.get('name')
    embedding_model = data.get('embedding_model')
    metadata = data.get('metadata')
    fileURL = data.get('fileURL')
    milvus_client = MilvusClient(
        collection_name=name
    )
    res = milvus_client.insert_vector_from_file(embedding_model, fileURL, metadata)

    return {
        "insert_count": res.insert_count,
        "delete_count": res.delete_count,
        "upsert_count": res.upsert_count,
        "success_count": res.succ_count,
        "err_count": res.err_count
    }


@app.post("/api/vector/query")
def query_vector():
    data = request.json
    name = data.get('name')
    expr = ''
    milvus_client = MilvusClient(
        collection_name=name
    )
    records = milvus_client.query_vector(
        expr=expr,
        offset=0,
        limit=100,
    )
    return {
        "records": records
    }


@app.post("/api/vector/search")
def search_vector():
    data = request.json
    name = data.get('name')
    embedding_model = data.get('embedding_model')
    expr = data.get('expr')
    q = data.get('q')
    embedding = generate_embedding_of_model(embedding_model, q)
    milvus_client = MilvusClient(
        collection_name=name
    )
    data = milvus_client.search_vector(embedding, expr, 10)
    return {
        "records": data,
    }


@app.post('/api/collections')
def create_collection():
    data = request.json
    role_name = data.get('role_name')
    name = data.get('name')
    embedding_model = data.get('embedding_model')
    dimension = data.get('dimension')
    create_milvus_collection(
        role_name,
        name,
        embedding_model,
        dimension
    )
    return {
        "success": True
    }


@app.delete("/api/collections/<string:name>/records/<string:pk>")
def delete_record(name, pk):
    milvus_client = MilvusClient(
        collection_name=name
    )
    result = milvus_client.delete_record(pk)
    return {
        "delete_count": result.delete_count
    }


@app.put("/api/collections/<string:name>/records/<string:pk>")
def upsert_record(name, pk):
    data = request.json
    text = data.get('text')
    metadata = data.get('metadata')
    embedding_model = data.get('embedding_model')
    embedding = generate_embedding_of_model(embedding_model, [text])
    milvus_client = MilvusClient(
        collection_name=name
    )
    result = milvus_client.upsert_record(pk, text, embedding, metadata)
    return {
        "upsert_count": result.upsert_count
    }
