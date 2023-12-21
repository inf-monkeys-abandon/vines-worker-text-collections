from src.database import CollectionTable
from src.milvus import MilvusClient
from src.utils import generate_embedding_of_model

BLOCK_NAME = 'search_vector'
BLOCK_DEF = {
    "type": "SIMPLE",
    "name": BLOCK_NAME,
    "categories": ['modelEnhance'],
    "displayName": '搜索向量数据',
    "description": '根据提供的文本对向量数据库进行相似性查找',
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
            "displayName": '相似性文本',
            "name": 'question',
            "type": 'string',
            "default": '',
            "required": True,
        },
        {
            "name": "docs",
            "type": "notice",
            "displayName": '过滤表达式用于对向量进行精准过滤，如 metadata["source"] == "example"，详细语法请见：[https://milvus.io/docs/json_data_type.md](https://milvus.io/docs/json_data_type.md)'
        },
        {
            "displayName": '过滤表达式',
            "name": 'expr',
            "type": 'string',
            "default": '',
            "required": False,
            "placeholder": 'metadata["source"] == "example"',
            "extra": ""
        },
        {
            "displayName": 'TopK',
            "name": 'topK',
            "type": 'number',
            "default": 3,
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
        {
            "name": "text",
            "displayName": "所有搜索的结果组合的字符串",
            "type": "string"
        }
    ],
    "extra": {
        "estimateTime": 5,
    },
}


def handler(task, workflow_context, credential_data=None):
    workflow_instance_id = task.get('workflowInstanceId')
    task_id = task.get('taskId')
    task_type = task.get('taskType')
    print(f"开始执行任务：workflow_instance_id={workflow_instance_id}, task_id={task_id}, task_type={task_type}")

    input_data = task.get("inputData")
    print(input_data)
    team_id = workflow_context.get('teamId')
    collection_name = input_data.get('collection')
    question = input_data.get('question')
    expr = input_data.get('expr')
    top_k = input_data.get('topK')

    app_id = workflow_context.get('APP_ID')
    table = CollectionTable(app_id=app_id)
    collection = table.find_by_name(team_id, name=collection_name)
    if not collection:
        raise Exception(f"数据集 {collection_name} 不存在或未授权")

    milvus_client = MilvusClient(
        app_id=app_id,
        collection_name=collection_name
    )
    embedding_model = collection.get('embeddingModel')
    embedding = generate_embedding_of_model(embedding_model, question)

    data = milvus_client.search_vector(embedding, expr, top_k)

    texts = [
        item['page_content'] for item in data
    ]
    text = '\n'.join(texts)

    return {
        "result": data,
        "text": text
    }
