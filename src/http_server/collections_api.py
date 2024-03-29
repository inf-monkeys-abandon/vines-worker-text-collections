import json

from .server import app
from flask import request
from vines_worker_sdk.server.exceptions import ClientException
from src.database import CollectionTable, FileProcessProgressTable, FileRecord
from bson.json_util import dumps
from src.utils import generate_short_id, get_dimension_by_embedding_model, generate_random_string
from src.es import ESClient


@app.post('/api/vector/collections')
def create_collection():
    app_id = request.app_id
    data = request.json
    displayName = data.get('displayName')
    logo = data.get('logo')
    name = generate_random_string()
    embedding_model = data.get('embeddingModel')
    metadata_fields = data.get('metadataFields', None)
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

    # 在 es 中创建 template
    es_client = ESClient(
        app_id=app_id,
        index_name=name
    )
    es_client.create_es_index(dimension)
    table.insert_one(
        creator_user_id=user_id,
        team_id=team_id,
        name=name,
        display_name=displayName,
        description=description,
        embedding_model=embedding_model,
        dimension=dimension,
        logo=logo,
        metadata_fields=metadata_fields
    )

    return {
        "success": True,
        "name": name
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
    file_record_table = FileRecord(app_id=app_id)
    for item in data:
        es_client = ESClient(app_id=app_id, index_name=item['name'])
        entity_count = es_client.count_documents()
        item['entityCount'] = entity_count
        file_count = file_record_table.get_file_count(team_id, item['name'])
        item['fileCount'] = file_count
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

    table.update_by_name(
        team_id,
        name,
        description=description,
        display_name=display_name,
        logo=logo,
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

    embedding_model = collection.get('embeddingModel')
    dimension = collection.get('dimension')
    new_collection_name = generate_short_id()
    description = collection.get('description')

    # 在 es 中创建 template
    es_client = ESClient(app_id=app_id, index_name=name)
    es_client.create_es_index(
        dimension
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
        metadata_fields=collection.get('metadataFields')
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
    es_client = ESClient(app_id=app_id, index_name=name)
    es_client.delete_index()
    return {
        "success": True
    }


@app.post("/api/vector/collections/<string:name>/delete-all-data")
def empty_collection(name):
    app_id = request.app_id
    es_client = ESClient(app_id=app_id, index_name=name)
    # 删除索引
    es_client.delete_index()
    # 重新创建个新的
    table = CollectionTable(
        app_id=app_id
    )
    collection = table.find_by_name_without_team(name)
    es_client.create_es_index(
        dimension=collection['dimension']
    )
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
