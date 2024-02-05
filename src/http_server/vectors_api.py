from flask import request
from src.utils import generate_embedding_of_model, generate_md5
from .server import app
from src.database import CollectionTable, FileProcessProgressTable
from vines_worker_sdk.server.exceptions import ServerException, ClientException
import uuid
from src.queue import submit_task, PROCESS_FILE_QUEUE_NAME
from src.es import ESClient
from ..utils.oss.tos import TOSClient
from ..utils.oss.aliyunoss import AliyunOSSClient


@app.post("/api/vector/collections/<string:name>/records")
def save_vector(name):
    team_id = request.team_id
    user_id = request.user_id
    app_id = request.app_id
    table = CollectionTable(
        app_id=app_id
    )
    collection = table.find_by_name(team_id, name)
    embedding_model = collection["embeddingModel"]

    data = request.json
    text = data.get("text")
    file_url = data.get("fileURL")
    metadata = data.get("metadata", {})
    oss_config = data.get('ossConfig', None)
    metadata["userId"] = user_id

    es_client = ESClient(app_id=app_id, index_name=name)

    if text:
        delimiter = data.get('delimiter')
        if delimiter:
            delimiter = delimiter.replace('\\n', '\n')
            text_list = text.split(delimiter)
            text_list = [
                {
                    "page_content": item,
                    "metadata": metadata
                } for item in text_list
            ]
            es_client.insert_texts_batch(embedding_model, text_list)
            return {
                'inserted': len(text_list)
            }
        else:
            embedding = generate_embedding_of_model(embedding_model, [text])
            pk = generate_md5(text)
            es_client.upsert_document(pk, {
                "page_content": text,
                "metadata": metadata,
                "embeddings": embedding[0]
            })
            table.add_metadata_fields_if_not_exists(
                team_id, name, metadata.keys()
            )
            return {
                "pk": pk
            }
    elif file_url or oss_config:
        split = data.get('split', {})
        params = split.get('params', {})

        # json 文件
        jqSchema = params.get('jqSchema', None)

        # 非 json 文件
        pre_process_rules = params.get('preProcessRules', [])
        segmentParams = params.get('segmentParams', {})
        chunk_overlap = segmentParams.get('segmentChunkOverlap', 10)
        chunk_size = segmentParams.get('segmentMaxLength', 1000)
        separator = segmentParams.get('segmentSymbol', "\n\n")
        task_id = str(uuid.uuid4())

        progress_table = FileProcessProgressTable(app_id)
        progress_table.create_task(
            team_id=team_id, collection_name=name, task_id=task_id
        )
        submit_task(PROCESS_FILE_QUEUE_NAME, {
            'app_id': app_id,
            'team_id': team_id,
            'user_id': user_id,
            'collection_name': name,
            'embedding_model': embedding_model,
            'file_url': file_url,
            'oss_config': oss_config,
            'metadata': metadata,
            'task_id': task_id,
            'chunk_size': chunk_size,
            'chunk_overlap': chunk_overlap,
            'separator': separator,
            'pre_process_rules': pre_process_rules,
            'jqSchema': jqSchema
        })
        return {"taskId": task_id}
    else:
        raise ServerException("非法的请求参数，请传入 text 或者 fileUrl")


@app.post("/api/vector/collections/<string:name>/records/oss-import-test")
def oss_import_test(name):
    data = request.json
    oss_type, oss_config = data.get('ossType'), data.get('ossConfig')
    if oss_type == 'TOS':
        endpoint, region, bucket_name, accessKeyId, accessKeySecret, baseFolder = oss_config.get(
            'endpoint'), oss_config.get('region'), oss_config.get('bucketName'), oss_config.get(
            'accessKeyId'), oss_config.get('accessKeySecret'), oss_config.get('baseFolder')
        tos_client = TOSClient(
            endpoint,
            region,
            bucket_name,
            accessKeyId,
            accessKeySecret,
        )
        result = tos_client.test_connection(
            baseFolder,
        )
        return {
            "result": result
        }
    elif oss_type == 'ALIYUNOSS':
        endpoint, bucket_name, accessKeyId, accessKeySecret, baseFolder = oss_config.get(
            'endpoint'), oss_config.get('bucketName'), oss_config.get(
            'accessKeyId'), oss_config.get('accessKeySecret'), oss_config.get('baseFolder')
        aliyunoss_client = AliyunOSSClient(
            endpoint=endpoint,
            bucket_name=bucket_name,
            access_key=accessKeyId,
            secret_key=accessKeySecret
        )
        result = aliyunoss_client.test_connection(
            baseFolder,
        )
        return {
            "result": result
        }
    else:
        raise Exception(f"不支持的 oss 类型: {oss_type}")


@app.post("/api/vector/collections/<string:name>/records/upsert")
def upsert_vector_batch(name):
    app_id = request.app_id
    table = CollectionTable(
        app_id=app_id
    )
    collection = table.find_by_name_without_team(name)
    if not collection:
        raise ClientException(f"向量数据库 {name} 不存在")
    embedding_model = collection.get("embeddingModel")
    es_client = ESClient(app_id=app_id, collection_name=name)
    list = request.json
    texts = [item["text"] for item in list]
    embeddings = generate_embedding_of_model(embedding_model, texts)
    es_documents = []
    for (index, item) in enumerate(list):
        es_documents.append({
            "_id": item['pk'],
            "_source": {
                "page_content": item['text'],
                "metadata": item['metadata'],
                "embeddings": embeddings[index]
            }
        })
    es_client.upsert_documents_batch(
        es_documents
    )
    return {"success": True}


@app.post("/api/vector/collections/<string:name>/full-text-search")
def full_text_search(name):
    app_id = request.app_id
    data = request.json
    query = data.get("query", None)
    es_client = ESClient(app_id=app_id, index_name=name)
    from_ = data.get("from", 0)
    size = data.get("size", 30)
    metadata_filter = data.get('metadataFilter', None)
    sort_by_created_at = data.get('sortByCreatedAt', False)
    hits = es_client.full_text_search(
        query=query,
        from_=from_,
        size=size,
        metadata_filter=metadata_filter,
        sort_by_created_at=sort_by_created_at
    )
    return {"hits": hits}


@app.post("/api/vector/collections/<string:name>/vector-search")
def vector_search(name):
    team_id = request.team_id
    app_id = request.app_id
    table = CollectionTable(
        app_id=app_id
    )
    data = request.json
    collection = table.find_by_name(team_id, name)
    embedding_model = collection["embeddingModel"]
    query = data.get("query")
    limit = data.get("limit", 30)
    metadataFilter = data.get('metadataFilter', None)
    embedding = generate_embedding_of_model(embedding_model, query)
    es_client = ESClient(app_id=app_id, index_name=name)
    hits = es_client.vector_search(embedding, limit, metadata_filter=metadataFilter)
    return {"hits": hits}


@app.delete("/api/vector/collections/<string:name>/records/<string:pk>")
def delete_record(name, pk):
    app_id = request.app_id
    es_client = ESClient(app_id=app_id, index_name=name)
    result = es_client.delete_es_document(pk)
    return {"result": result.body}


@app.put("/api/vector/collections/<string:name>/records/<string:pk>")
def upsert_record(name, pk):
    data = request.json
    team_id = request.team_id
    app_id = request.app_id
    table = CollectionTable(
        app_id=app_id
    )
    text = data.get("text")
    metadata = data.get("metadata")
    collection = table.find_by_name(team_id, name)
    embedding_model = collection["embeddingModel"]
    embedding = generate_embedding_of_model(embedding_model, [text])
    es_client = ESClient(
        app_id=app_id,
        index_name=name
    )
    result = es_client.upsert_document(
        pk=pk,
        document={
            "page_content": text,
            "metadata": metadata,
            "embeddings": embedding[0]
        }
    )
    return {"result": result.body}
