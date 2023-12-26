import json

from .server import app
from flask import request
from vines_worker_sdk.server.exceptions import ClientException
from src.database import CollectionTable, FileProcessProgressTable
from src.milvus import create_milvus_collection, drop_milvus_collection, rename_collection, get_entity_count_batch, \
    get_entity_count
from bson.json_util import dumps
from src.utils import generate_short_id, get_dimension_by_embedding_model, generate_random_string
from .users_api import init_milvus_user_if_not_exists
import traceback


@app.post('/api/vector/collections')
def create_collection():
    app_id = request.app_id
    data = request.json
    displayName = data.get('displayName')
    logo = data.get('logo')
    name = generate_random_string()
    embedding_model = data.get('embeddingModel')

    index_type = data.get('indexType')
    if not index_type:
        raise ClientException("请指定 index 类型")
    index_param = data.get('indexParam')
    if not index_param:
        raise ClientException("请指定 index 参数")
    description = data.get('description', '')
    dimension = get_dimension_by_embedding_model(embedding_model)
    table = CollectionTable(
        app_id=app_id
    )
    conflict = table.check_name_conflicts(name)
    if conflict:
        raise ClientException(f"唯一标志 {name} 已被占用，请换一个")

    user_id = request.user_id
    team_id = request.team_id

    init_milvus_user_if_not_exists(app_id, team_id)

    # 在 milvus 中创建
    create_milvus_collection(
        app_id,
        name,
        index_type,
        index_param,
        description,
        dimension
    )
    table.insert_one(
        creator_user_id=user_id,
        team_id=team_id,
        name=name,
        display_name=displayName,
        description=description,
        embedding_model=embedding_model,
        dimension=dimension,
        logo=logo,
        index_type=index_type,
        index_param=index_param
    )

    return {
        "success": True
    }


@app.get("/api/vector/collections")
def list_collections():
    team_id = request.team_id
    app_id = request.app_id
    table = CollectionTable(
        app_id=app_id
    )
    data = table.find_by_team(team_id=team_id)
    data = json.loads(dumps(data))
    progress_table = FileProcessProgressTable(app_id=app_id)
    for item in data:
        item['entityCount'] = get_entity_count(app_id, item['name'])
        item['fileCount'] = progress_table.get_file_count(team_id, item['name'])
    return data


@app.get("/api/vector/collections/<string:name>")
def get_collection_detail(name):
    team_id = request.team_id
    app_id = request.app_id
    table = CollectionTable(
        app_id=app_id
    )
    data = table.find_by_name(team_id, name)
    return dumps(data)


@app.put("/api/vector/collections/<string:name>")
def update_collection(name):
    team_id = request.team_id
    app_id = request.app_id
    table = CollectionTable(
        app_id=app_id
    )
    collection = table.find_by_name(team_id, name)
    if not collection:
        raise ClientException("数据集不存在")

    data = request.json
    description = data.get('description')
    display_name = data.get('displayName')
    logo = data.get('logo')
    new_name = data.get('name', None)
    original_name = collection['name']
    if new_name and new_name == original_name:
        new_name = None

    if new_name:
        conflict = table.check_name_conflicts(new_name)
        if conflict:
            raise Exception(f"{new_name} 已经存在")

    if new_name:
        rename_collection(app_id, name, new_name)

    table.update_by_name(
        team_id,
        name,
        description=description,
        display_name=display_name,
        logo=logo,
        new_name=new_name
    )
    return {
        "success": True
    }


@app.post("/api/vector/collections/<string:name>/authorize")
def authorize_collection(name):
    app_id = request.app_id
    table = CollectionTable(
        app_id=app_id
    )
    if not request.is_super_user:
        raise Exception("无权限操作")
    collection = table.find_by_name_without_team(name)
    if not collection:
        raise ClientException("数据集不存在")

    data = request.json
    team_id = data.get('team_id')
    table.authorize_target(
        name,
        team_id,
    )
    return {
        "success": True
    }


@app.post("/api/vector/collections/<string:name>/copy")
def copy_collection(name):
    app_id = request.app_id
    table = CollectionTable(
        app_id=app_id
    )
    if not request.is_super_user:
        raise Exception("无权限操作")
    collection = table.find_by_name_without_team(name)
    if not collection:
        raise ClientException("数据集不存在")

    data = request.json
    team_id = data.get('team_id')
    user_id = data.get('user_id')
    init_milvus_user_if_not_exists(app_id, team_id)

    # 在 milvus 中创建
    embedding_model = collection.get('embeddingModel')
    dimension = collection.get('dimension')
    new_collection_name = generate_short_id()
    description = collection.get('description')
    index_type = collection.get('indexType', 'IVF_FLAT')
    index_param = collection.get('indexParma', {
        "metric_type": "L2",
        "params": {
            "nlist": 128
        }
    })
    create_milvus_collection(
        app_id=app_id,
        name=new_collection_name,
        index_type=index_type,
        index_param=index_param,
        description=description,
        dimension=dimension
    )

    table.insert_one(
        creator_user_id=user_id,
        team_id=team_id,
        name=new_collection_name,
        display_name=collection.get('displayName'),
        description=description,
        logo=collection.get('logo'),
        embedding_model=embedding_model,
        dimension=dimension,
        index_type=index_type,
        index_param=index_param
    )
    return {
        "name": new_collection_name
    }


@app.delete("/api/vector/collections/<string:name>")
def delete_collection(name):
    team_id = request.team_id
    app_id = request.app_id
    table = CollectionTable(
        app_id=app_id
    )
    table.delete_by_name(team_id, name)

    drop_milvus_collection(app_id, name)
    return {
        "success": True
    }


@app.get("/api/vector/collections/<string:name>/tasks")
def list_tasks(name):
    team_id = request.team_id
    app_id = request.app_id
    table = FileProcessProgressTable(
        app_id=app_id
    )
    data = table.list_tasks(
        team_id=team_id,
        collection_name=name
    )
    return dumps(data)


@app.get("/api/vector/collections/<string:name>/tasks/<string:task_id>")
def get_task_detail(name, task_id):
    team_id = request.team_id
    app_id = request.app_id
    table = FileProcessProgressTable(
        app_id=app_id
    )
    data = table.get_task(
        team_id=team_id,
        collection_name=name,
        task_id=task_id
    )
    return dumps(data)
