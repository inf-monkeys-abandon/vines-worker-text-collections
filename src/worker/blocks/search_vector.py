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
