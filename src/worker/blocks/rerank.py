from FlagEmbedding import FlagReranker
from src.utils import ROOT_FOLDER
import os

BLOCK_NAME = 'reranker'
BLOCK_DEF = {
    "type": "SIMPLE",
    "name": BLOCK_NAME,
    "categories": ['query'],
    "displayName": '相关性重排序（reranker）',
    "description": '基于 BAAI/bge-reranker-large 模型对文本进行相似度重排序',
    "icon": 'emoji:💿:#e58c3a',
    "input": [
        {
            "displayName": 'Query',
            "name": 'query',
            "type": 'string',
            "default": '',
            "required": True
        },
        {
            "displayName": '文档列表',
            "name": 'array',
            "type": 'collection',
            "required": True,
        },
        {
            "displayName": 'Top-K 数值',
            "description": "不设置则返回所有",
            "name": 'topK',
            "type": 'number'
        }
    ],
    "output": [
        {
            "name": 'sortedArray',
            "displayName": '重排后的 Top-K 列表',
            "type": 'collection'
        },
        {
            "name": "scores",
            "displayName": "计算的分数列表",
            "type": "string"
        },
        {
            "name": "str",
            "displayName": "重排后的 Top-K 列表组合的字符串",
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
    query = input_data.get('query')
    array = input_data.get('array')
    top_k = input_data.get('topK')

    reranker = FlagReranker(
        os.path.join(ROOT_FOLDER, 'models/bge-reranker-large'),
        use_fp16=True
    )
    args = [
        [query, item] for item in array
    ]

    scores = reranker.compute_score(args)
    sorted_array = [item for score, item in sorted(zip(scores, array), reverse=True)]

    if top_k != None:
        sorted_array = sorted_array[:top_k]
    return {
        "scores": scores,
        "sortedArray": sorted_array,
        "str": "\n".join(sorted_array)
    }
