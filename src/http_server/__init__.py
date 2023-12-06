import time

from vines_worker_sdk.server import create_server
from vines_worker_sdk.utils.files import ensure_directory_exists
from flask import request
import os
from langchain.text_splitter import CharacterTextSplitter
from langchain.document_loaders import TextLoader, PyMuPDFLoader, CSVLoader, UnstructuredFileLoader, \
    UnstructuredMarkdownLoader, \
    JSONLoader
from src.milvus import MilvusClient, create_milvus_collection
from src.oss import oss_client
from src.utils import generate_embedding_of_model

SERVICE_AUTHENTICATION_TOKEN = os.environ.get("SERVICE_AUTHENTICATION_TOKEN")
if not SERVICE_AUTHENTICATION_TOKEN:
    raise Exception("请在环境变量中配置 SERVICE_AUTHENTICATION_TOKEN")

app = create_server(
    service_token=SERVICE_AUTHENTICATION_TOKEN,
    import_name="vines-worker-milvus",
)


@app.post("/api/vector/save-vector-from-text")
def save_vector_from_text():
    data = request.json
    name = data.get('name')
    embedding_model = data.get('embedding_model')
    text = data.get('text')
    metadata = data.get('metadata', {})
    embedding = generate_embedding_of_model(embedding_model, [text])
    client = MilvusClient(
        collection_name=name
    )
    res = client.insert_vectors([text], embedding, [metadata])
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
    fileURL = data.get('fileURL')

    folder = ensure_directory_exists("./download")
    file_path = oss_client.download_file(fileURL, folder)

    file_ext = file_path.split('.')[-1]
    if file_ext == '.pdf':
        loader = PyMuPDFLoader(file_path=file_path)
    elif file_ext == '.csv':
        loader = CSVLoader(file_path=file_path)
    elif file_ext == '.txt':
        loader = TextLoader(file_path=file_path)
    elif file_ext == '.md':
        loader = UnstructuredMarkdownLoader(file_path=file_path)
    elif file_ext == '.json' or file_ext == '.jsonl':
        jq_schema = '.[]'
        loader = JSONLoader(file_path=file_path, jq_schema=jq_schema)
    else:
        loader = UnstructuredFileLoader(file_path=file_path)

    documents = loader.load()
    text_splitter = CharacterTextSplitter(chunk_size=2000, chunk_overlap=0)
    texts = text_splitter.split_documents(documents)
    metadatas = [
        {
            "source": fileURL,
            "createdAt": int(time.time())
        } for _ in texts
    ]
    texts = [text.page_content for text in texts]

    embeddings = generate_embedding_of_model(embedding_model, texts)

    client = MilvusClient(
        collection_name=name
    )
    res = client.insert_vectors(texts, embeddings, metadatas)

    print(res)
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
    client = MilvusClient(
        collection_name=name
    )
    records = client.query_vector(
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
    client = MilvusClient(
        collection_name=name
    )
    data = client.search_vector(embedding, expr, 10)
    return {
        "records": data,
    }


@app.post('/api/collections')
def create_collection():
    data = request.json
    name = data.get('name')
    embedding_model = data.get('embedding_model')
    dimension = data.get('dimension')
    create_milvus_collection(
        name,
        embedding_model,
        dimension
    )
    return {
        "success": True
    }


@app.delete("/api/collections/<string:name>/records/<string:pk>")
def delete_record(name, pk):
    client = MilvusClient(
        collection_name=name
    )
    result = client.delete_record(pk)
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
    client = MilvusClient(
        collection_name=name
    )
    result = client.upsert_record(pk, text, embedding, metadata)
    return {
        "upsert_count": result.upsert_count
    }
