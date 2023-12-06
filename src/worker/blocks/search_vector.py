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
    ],
    "extra": {
        "estimateTime": 3,
    },
}


def handler(task):
    workflow_instance_id = task.get('workflowInstanceId')
    task_id = task.get('taskId')
    print(f"开始执行任务：workflow_instance_id={workflow_instance_id}, task_id={task_id}")

    input_data = task.get("inputData")
    print(input_data)

    collection = input_data.get('collection')
    question = input_data.get('question')
    top_k = input_data.get('topK')

    milvus_client = MilvusClient(
        collection_name=collection
    )
    embedding_model = milvus_client.collection.description
    print(embedding_model)
    embedding = generate_embedding_of_model(embedding_model, question)

    data = milvus_client.search_vector(embedding, top_k)

    return {
        "result": data
    }
