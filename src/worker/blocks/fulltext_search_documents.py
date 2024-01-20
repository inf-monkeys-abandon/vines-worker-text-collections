from src.es import search_records

BLOCK_NAME = 'fulltext_search_documents'
BLOCK_DEF = {
    "type": "SIMPLE",
    "name": BLOCK_NAME,
    "categories": ['query'],
    "displayName": '全文检索',
    "description": '对文本进行全文关键字搜索，返回最匹配的文档列表',
    "icon": 'emoji:💿:#e58c3a',
    "input": [
        {
            "displayName": '文本数据库',
            "name": 'collection',
            "type": 'selectVectorCollection',
            "default": '',
            "required": True,
            "assetType": 'vectorDatabase',
        },
        {
            "displayName": '关键词',
            "name": 'query',
            "type": 'string',
            "default": '',
            "required": True,
        },
        {
            "name": "docs",
            "type": "notice",
            "displayName": """使用 ES 搜索过滤表达式用于对文本进行精准过滤。示例：
```json
{
    "term": {
        "metadata.source": "文件名称"
    }
}
```
            """
        },
        {
            "displayName": '过滤表达式',
            "name": 'expr',
            "type": 'jsonObject',
            "default": '',
            "required": False,
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
    collection_name = input_data.get('collection')
    query = input_data.get('query')
    expr = input_data.get('expr')
    top_k = input_data.get('topK', 10)

    if not isinstance(top_k, int):
        raise Exception("topK 必须是一个数字")

    app_id = workflow_context.get('APP_ID')

    result = search_records(
        app_id=app_id,
        index_name=collection_name,
        query=query,
        expr=expr,
        size=top_k
    )

    texts = [
        item['page_content'] for item in result
    ]
    text = '\n'.join(texts)

    return {
        "result": result,
        "text": text
    }
