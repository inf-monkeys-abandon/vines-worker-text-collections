from src.milvus import MilvusClient
from src.utils import generate_embedding_of_model, generate_md5
from src.database import CollectionTable, FileProcessProgressTable

BLOCK_NAME = 'insert_vector'
BLOCK_DEF = {
    "type": "SIMPLE",
    "name": BLOCK_NAME,
    "categories": ['query', 'db'],
    "displayName": '写入文本数据',
    "description": '写入文本数据到向量数据库',
    "icon": 'emoji:💿:#e58c3a',
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
        "estimateTime": 30,
    },
}


def handler(task, workflow_context, credential_data=None):
    workflow_id = task.get('workflowType')
    workflow_instance_id = task.get('workflowInstanceId')
    task_id = task.get('taskId')
    task_type = task.get('taskType')
    print(f"开始执行任务：workflow_id={workflow_id}, workflow_instance_id={workflow_instance_id}, task_id={task_id} task_type={task_type}")

    input_data = task.get("inputData")
    print(input_data)

    collection_name = input_data.get('collection')
    inputType = input_data.get('inputType')
    text = input_data.get('text')
    fileUrl = input_data.get('fileUrl')
    metadata = input_data.get('metadata')

    if not metadata:
        metadata = {}

    team_id = workflow_context.get('teamId')
    app_id = workflow_context.get('APP_ID')
    table = CollectionTable(app_id=app_id)
    collection = table.find_by_name(team_id, name=collection_name)
    if not collection:
        raise Exception(f"数据集 {collection_name} 不存在或未授权")

    if metadata and isinstance(metadata, dict):
        metadata.update({
            "workflowId": workflow_id
        })

    table.add_metadata_fields_if_not_exists(team_id, collection_name, metadata.keys())

    milvus_client = MilvusClient(
        app_id=app_id,
        collection_name=collection_name
    )
    embedding_model = collection.get('embeddingModel')

    if inputType == 'text':
        embedding = generate_embedding_of_model(embedding_model, [text])
        pk = generate_md5(text)
        res = milvus_client.upsert_record_batch([pk], [text], embedding, [metadata])
    elif inputType == 'fileUrl':
        progress_table = FileProcessProgressTable(app_id=app_id)
        progress_table.create_task(
            team_id=team_id,
            collection_name=collection_name,
            task_id=task_id
        )
        try:
            res = milvus_client.insert_vector_from_file(embedding_model, fileUrl, metadata, task_id)
        except Exception as e:
            progress_table.mark_task_failed(task_id=task_id, message=str(e))
            raise Exception(e)
    else:
        raise Exception("不合法的 inputType: ", inputType)

    return {
        "insert_count": res.insert_count,
        "delete_count": res.delete_count,
        "upsert_count": res.upsert_count,
        "success_count": res.succ_count,
        "err_count": res.err_count
    }
