from src.es import ESClient
from vines_worker_sdk.conductor.worker import Worker


class FullTextSearchWorker(Worker):
    block_name = 'fulltext_search_documents'
    block_def = {
        "type": "SIMPLE",
        "name": block_name,
        "categories": ['query'],
        "displayName": '文本全文搜索',
        "description": '对文本进行全文关键字搜索，返回最匹配的文档列表',
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
                "required": True
            },
            {
                "displayName": '关键词',
                "name": 'query',
                "type": 'string',
                "default": '',
                "required": False,
            },
            {
                "displayName": 'TopK',
                "name": 'topK',
                "type": 'number',
                "default": 3,
                "required": False,
            },
            {
                "displayName": '数据过滤方式',
                "name": 'filterType',
                "type": 'options',
                "options": [
                    {
                        "name": "简单形式",
                        "value": "simple"
                    },
                    {
                        "name": "ES 表达式",
                        "value": "es-expression"
                    }
                ],
                "default": 'simple',
                "required": False,
            },
            {
                "displayName": '根据元数据的字段进行过滤',
                "name": 'metadata_filter',
                "type": 'json',
                "typeOptions": {
                    "multiFieldObject": True,
                    "multipleValues": False
                },
                "default": '',
                "required": False,
                "description": "根据元数据的字段进行过滤",
                "displayOptions": {
                    "show": {
                        "filterType": [
                            "simple"
                        ]
                    }
                }
            },
            {
                "name": "docs",
                "type": "notice",
                "displayName": """使用 ES 搜索过滤表达式用于对文本进行精准过滤。\n示例：
```json
{
    "term": {
        "metadata.filename.keyword": "文件名称"
    }
}
```
            """,
                "displayOptions": {
                    "show": {
                        "filterType": [
                            "es-expression"
                        ]
                    }
                }
            },
            {
                "displayName": '过滤表达式',
                "name": 'expr',
                "type": 'json',
                "required": False,
                "displayOptions": {
                    "show": {
                        "filterType": [
                            "es-expression"
                        ]
                    }
                }
            },
            {
                "displayName": '是否按照创建时间进行排序',
                "name": 'orderByCreatedAt',
                "type": 'boolean',
                "required": False,
                "default": False
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
        collection_name = input_data.get('collection')
        query = input_data.get('query')
        expr = input_data.get('expr')
        top_k = input_data.get('topK', 10)
        metadata_filter = input_data.get('metadata_filter')
        order_by_created_at = input_data.get("orderByCreatedAt")

        if not isinstance(top_k, int):
            raise Exception("topK 必须是一个数字")

        app_id = workflow_context.get('APP_ID')

        es_client = ESClient(
            app_id=app_id,
            index_name=collection_name
        )
        result = es_client.full_text_search(
            query=query,
            expr=expr,
            metadata_filter=metadata_filter,
            size=top_k,
            sort_by_created_at=order_by_created_at
        )
        result = [{
            'page_content': item['_source']['page_content'],
            "metadata": item['_source']['metadata']
        } for item in result]
        texts = [
            item['page_content'] for item in result
        ]
        text = '\n'.join(texts)

        return {
            "result": result,
            "text": text
        }
