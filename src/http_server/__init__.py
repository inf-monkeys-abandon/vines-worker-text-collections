from FlagEmbedding import FlagModel
from vines_worker_sdk.server import create_server
from vines_worker_sdk.utils.files import ensure_directory_exists
from flask import request
import os
from langchain.text_splitter import CharacterTextSplitter
from langchain.document_loaders import TextLoader, PyMuPDFLoader, CSVLoader, UnstructuredFileLoader, \
    UnstructuredMarkdownLoader, \
    JSONLoader
from ..milvus import MilvusClient, create_milvus_collection
from ..oss import oss_client

SERVICE_TOKEN = os.environ.get("SERVICE_TOKEN")
if not SERVICE_TOKEN:
    raise Exception("请在环境变量中配置 SERVICE_TOKEN")

app = create_server(
    service_token=SERVICE_TOKEN,
    import_name="vines-worker-milvus",
)


def generate_embedding_of_model(model_name, sentences):
    model = FlagModel(
        model_name,
        use_fp16=True
    )  # Setting use_fp16 to True speeds up computation with a slight performance degradation
    embeddings = model.encode(sentences)
    return embeddings


@app.post("/api/vector/save-vector-from-text")
def save_vector_from_text():
    data = request.json
    name = data.get('name')
    model_name = data.get('model_name')
    text = data.get('text')
    embedding = generate_embedding_of_model(model_name, [text])
    client = MilvusClient(
        collection_name=name
    )
    res = client.insert_vectors([text], [embedding])
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
    model_name = data.get('model_name')
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
    texts = [text.page_content for text in texts]
    embeddings = generate_embedding_of_model(model_name, texts)

    client = MilvusClient(
        collection_name=name
    )
    res = client.insert_vectors(texts, embeddings)

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
    output_fields = data.get('output_fields')
    client = MilvusClient(
        collection_name=name
    )
    records = client.query_vector(
        expr=expr,
        output_fields=output_fields,
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
    model_name = data.get('model_name')
    q = data.get('q')
    embedding = generate_embedding_of_model(model_name, q)
    client = MilvusClient(
        collection_name=name
    )
    data = client.search_vector(embedding, 10)
    return {
        "records": data,
    }


@app.post('/api/collections')
def create_collection():
    data = request.json
    name = data.get('name')
    description = data.get('description')
    dimension = data.get('dimension')
    create_milvus_collection(
        name,
        description,
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
    model_name = data.get('model_name')
    embedding = generate_embedding_of_model(model_name, [text])
    client = MilvusClient(
        collection_name=name
    )
    result = client.upsert_record(pk, text, embedding)
    return {
        "upsert_count": result.upsert_count
    }
