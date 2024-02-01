from src.utils import generate_embedding_of_model, generate_md5
from src.database import CollectionTable, FileProcessProgressTable
from src.es import ESClient

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
            "type": 'string',
            "typeOptions": {
                "assetType": 'text-collection'
            },
            "default": '',
            "required": True
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
            "displayName": '元数据类型',
            "name": 'metadataType',
            "type": 'options',
            "options": [
                {
                    "name": "Key-Value",
                    "value": "kv"
                },
                {
                    "name": "纯 JSON",
                    "value": "json"
                }
            ],
            "default": 'kv',
            "required": False,
            "description": "根据元数据的字段进行过滤"
        },
        {
            "displayName": '元数据',
            "name": 'metadata',
            "type": 'json',
            "typeOptions": {
                "multipleValues": False,
                "multiFieldObject": True
            },
            "default": '',
            "required": False,
            "description": "根据元数据的字段进行过滤",
            "displayOptions": {
                "show": {
                    "metadataType": ['kv']
                }
            }
        },
        {
            "displayName": '元数据',
            "name": 'metadata',
            "type": 'json',
            "default": {},
            "required": False,
            "displayOptions": {
                "show": {
                    "metadataType": ['json']
                }
            }
        },
    ],
    "output": [
        {
            "name": 'insert_count',
            "displayName": '写入的数目',
            "type": 'number',
        },
    ],
    "extra": {
        "estimateTime": 5,
    },
}


def handler(task, workflow_context, credential_data=None):
    workflow_id = task.get('workflowType')
    workflow_instance_id = task.get('workflowInstanceId')
    task_id = task.get('taskId')
    task_type = task.get('taskType')
    print(
        f"开始执行任务：workflow_id={workflow_id}, workflow_instance_id={workflow_instance_id}, task_id={task_id} task_type={task_type}")

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

    es_client = ESClient(
        app_id=app_id,
        index_name=collection_name
    )
    embedding_model = collection.get('embeddingModel')
    inserted_count = 0
    if inputType == 'text':
        embedding = generate_embedding_of_model(embedding_model, [text])
        pk = generate_md5(text)
        es_client.upsert_documents_batch([
            {
                "_id": pk,
                "_source": {
                    "page_content": text,
                    "metadata": metadata,
                    "embeddings": embedding[0]
                }
            }
        ])
        inserted_count = 1
    elif inputType == 'fileUrl':
        progress_table = FileProcessProgressTable(app_id=app_id)
        progress_table.create_task(
            team_id=team_id,
            collection_name=collection_name,
            task_id=task_id
        )
        try:
            inserted_count = es_client.insert_vector_from_file(
                team_id,
                embedding_model, fileUrl, metadata, task_id)
        except Exception as e:
            progress_table.mark_task_failed(task_id=task_id, message=str(e))
            raise Exception(e)
    else:
        raise Exception("不合法的 inputType: ", inputType)

    return {
        "insert_count": inserted_count
    }
