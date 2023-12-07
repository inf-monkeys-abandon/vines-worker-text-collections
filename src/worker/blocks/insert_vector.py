from src.milvus import MilvusClient
from src.utils import generate_embedding_of_model
from src.database import CollectionTable

BLOCK_NAME = 'insert_vector'
BLOCK_DEF = {
    "type": "SIMPLE",
    "name": BLOCK_NAME,
    "categories": ['modelEnhance'],
    "displayName": '写入向量数据',
    "description": '在向量数据库中写入向量数据',
    "icon": 'emoji:💽:#c5b1e1',
    "input": [
        {
            "displayName": '向量数据库',
            "name": 'collection',
            "type": 'selectVectorCollection',
            "default": '',
            "required": True,
            "assetType": 'vectorDatabase',
        },
        {
            "displayName": '输入类型',
            "name": 'inputType',
            "type": 'options',
            "options": [
                {
                    "name": '文本内容',
                    "value": 'text',
                },
                {
                    "name": '文件链接',
                    "value": 'fileUrl',
                },
            ],
            "default": 'text',
            "required": True,
        },
        {
            "displayName": '文本内容',
            "name": 'text',
            "type": 'string',
            "default": '',
            "required": True,
            "displayOptions": {
                "show": {
                    "inputType": ['text'],
                },
            },
        },
        {
            "displayName": '文件链接（支持输入 TXT, PDF, CSV, JSON, JSONL 文件类型）',
            "name": 'fileUrl',
            "type": 'string',
            "default": '',
            "required": True,
            "displayOptions": {
                "show": {
                    "inputType": ['fileUrl'],
                },
            },
        },
        {
            "name": "docs",
            "type": "notice",
            "displayName": '设置元数据之后，可以基于元数据对数据进行过滤，如 metadata["source"] == "example"，详细语法请见：[https://milvus.io/docs/json_data_type.md](https://milvus.io/docs/json_data_type.md)'
        },
        {
            "displayName": '元数据',
            "name": 'metadata',
            "type": 'jsonObject',
            "default": {},
            "required": False,
        },
    ],
    "output": [
        {
            "name": 'result',
            "displayName": '相似性集合',
            "type": 'collection',
            "properties": [
                {
                    "name": 'metadata',
                    "displayName": '元数据',
                    "type": 'any',
                },
                {
                    "name": 'page_content',
                    "displayName": '文本内容',
                    "type": 'string',
                },
            ],
        },
    ],
    "extra": {
        "estimateTime": 3,
    },
}


def handler(task, workflow_context):
    workflow_id = task.get('workflowType')
    workflow_instance_id = task.get('workflowInstanceId')
    task_id = task.get('taskId')
    print(f"开始执行任务：workflow_id={workflow_id}, workflow_instance_id={workflow_instance_id}, task_id={task_id}")

    input_data = task.get("inputData")
    print(input_data)

    collection_name = input_data.get('collection')
    inputType = input_data.get('inputType')
    text = input_data.get('text')
    fileUrl = input_data.get('fileUrl')
    metadata = input_data.get('metadata')
    team_id = workflow_context.get('teamId')

    collection = CollectionTable.find_by_name(team_id, name=collection_name)
    if not collection:
        raise Exception(f"数据集 {collection} 不存在")

    if metadata and isinstance(metadata, dict):
        metadata.update({
            "workflowId": workflow_id
        })

    CollectionTable.add_metadata_fields_if_not_exists(team_id, collection_name, metadata.keys())

    milvus_client = MilvusClient(
        collection_name=collection_name
    )
    embedding_model = collection.get('embeddingModel')

    if inputType == 'text':
        embedding = generate_embedding_of_model(embedding_model, [text])
        res = milvus_client.insert_vectors([text], embedding, [metadata])
    elif inputType == 'fileUrl':
        res = milvus_client.insert_vector_from_file(embedding_model, fileUrl, metadata)
    else:
        raise Exception("不合法的 inputType: ", inputType)

    return {
        "insert_count": res.insert_count,
        "delete_count": res.delete_count,
        "upsert_count": res.upsert_count,
        "success_count": res.succ_count,
        "err_count": res.err_count
    }
