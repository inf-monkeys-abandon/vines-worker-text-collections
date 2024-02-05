from src.database import CollectionTable
from src.utils import generate_embedding_of_model
from src.es import ESClient

from vines_worker_sdk.conductor.worker import Worker


class SearchVectorWorker(Worker):
    block_name = 'search_vector'
    block_def = {
        "type": "SIMPLE",
        "name": block_name,
        "categories": ['query'],
        "displayName": '文本向量搜索',
        "description": '根据提供的文本对进行相似性搜索',
        "icon": 'emoji:💿:#e58c3a',
        "input": [
            {
                "displayName": '文本数据库',
                "name": 'collection',
                "type": 'string',
                "typeOptions": {
                    "assetType": 'text-collection'
                },
                "default": '',
                "required": True,
            },
            {
                "displayName": '关键词',
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
            {
                "displayName": '根据元数据字段进行过滤',
                "name": 'metadata_filter',
                "type": 'json',
                "typeOptions": {
                    "multiFieldObject": True,
                    "multipleValues": False
                },
                "default": '',
                "required": False,
                "description": "根据元数据的字段进行过滤"
            },
        ],
        "output": [
            {
                "name": 'result',
                "displayName": '相似性集合',
                "type": 'json',
                "typeOptions": {
                    "multipleValues": True,
                },
                "properties": [
                    {
                        "name": 'metadata',
                        "displayName": '元数据',
                        "type": 'json',
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

    def handler(self, task, workflow_context, credential_data=None):
        workflow_instance_id = task.get('workflowInstanceId')
        task_id = task.get('taskId')
        task_type = task.get('taskType')
        print(f"开始执行任务：workflow_instance_id={workflow_instance_id}, task_id={task_id}, task_type={task_type}")

        input_data = task.get("inputData")
        print(input_data)
        team_id = workflow_context.get('teamId')
        collection_name = input_data.get('collection')
        question = input_data.get('question')
        top_k = input_data.get('topK')
        metadata_filter = input_data.get('metadata_filter', None)

        app_id = workflow_context.get('APP_ID')
        table = CollectionTable(app_id=app_id)
        collection = table.find_by_name(team_id, name=collection_name)
        if not collection:
            raise Exception(f"数据集 {collection_name} 不存在或未授权")

        es_client = ESClient(
            app_id=app_id,
            index_name=collection_name
        )
        embedding_model = collection.get('embeddingModel')
        embedding = generate_embedding_of_model(embedding_model, question)

        data = es_client.vector_search(embedding, top_k, metadata_filter)
        data = [{
            'page_content': item['_source']['page_content'],
            "metadata": item['_source']['metadata']
        } for item in data]
        texts = [
            item['page_content'] for item in data
        ]
        text = '\n'.join(texts)

        return {
            "result": data,
            "text": text
        }
