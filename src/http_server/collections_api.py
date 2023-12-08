from .server import app
from flask import request
from vines_worker_sdk.server.exceptions import ClientException
from src.database import CollectionTable
from src.milvus import create_milvus_collection, drop_milvus_collection
from bson.json_util import dumps
from src.utils import generate_short_id
from .users_api import init_milvus_user_if_not_exists


@app.post('/api/vector/collections')
def create_collection():
    data = request.json
    displayName = data.get('displayName')
    logo = data.get('logo')
    name = data.get('name')
    embedding_model = data.get('embeddingModel')
    description = data.get('description')

    dimension_map = {
        "BAAI/bge-base-zh-v1.5": 768
    }
    dimension = dimension_map.get(embedding_model)
    if not dimension:
        raise ClientException(f"不支持的 embedding 模型：{embedding_model}")

    conflict = CollectionTable.check_name_conflicts(name)
    if conflict:
        raise ClientException(f"唯一标志 {name} 已被占用，请换一个")

    user_id = request.user_id
    team_id = request.team_id

    init_milvus_user_if_not_exists(team_id)

    # 在 milvus 中创建
    role_name = f"team_{team_id}"
    create_milvus_collection(
        role_name,
        name,
        description,
        embedding_model,
        dimension
    )
    CollectionTable.insert_one(
        creator_user_id=user_id,
        team_id=team_id,
        name=name,
        display_name=displayName,
        description=description,
        embedding_model=embedding_model,
        dimension=dimension,
        logo=logo
    )

    return {
        "success": True
    }


@app.get("/api/vector/collections")
def list_collections():
    team_id = request.team_id
    data = CollectionTable.find_by_team(team_id=team_id)
    return dumps(data)


@app.get("/api/vector/collections/<string:name>")
def get_collection_detail(name):
    team_id = request.team_id
    data = CollectionTable.find_by_name(team_id, name)
    return dumps(data)


@app.put("/api/vector/collections/<string:name>")
def update_collection(name):
    team_id = request.team_id
    collection = CollectionTable.find_by_name(team_id, name)
    if not collection:
        raise ClientException("数据集不存在")

    data = request.json
    description = data.get('description')
    display_name = data.get('displayName')
    logo = data.get('logo')
    CollectionTable.update_by_name(
        team_id,
        name,
        description=description,
        display_name=display_name,
        logo=logo
    )
    return {
        "success": True
    }


@app.post("/api/vector/collections/<string:name>/authorize")
def authorize_collection(name):
    if not request.is_super_user:
        raise Exception("无权限操作")
    collection = CollectionTable.find_by_name_without_team(name)
    if not collection:
        raise ClientException("数据集不存在")

    data = request.json
    team_id = data.get('team_id')
    CollectionTable.authorize_target(
        name,
        team_id,
    )
    return {
        "success": True
    }


@app.post("/api/vector/collections/<string:name>/copy")
def copy_collection(name):
    if not request.is_super_user:
        raise Exception("无权限操作")
    collection = CollectionTable.find_by_name_without_team(name)
    if not collection:
        raise ClientException("数据集不存在")

    data = request.json
    team_id = data.get('team_id')
    user_id = data.get('user_id')
    init_milvus_user_if_not_exists(team_id)

    # 在 milvus 中创建
    embedding_model = collection.get('embeddingModel')
    dimension = collection.get('dimension')
    new_collection_name = generate_short_id()
    description = collection.get('description')
    role_name = f"team_{team_id}"
    create_milvus_collection(
        role_name,
        new_collection_name,
        description,
        embedding_model,
        dimension
    )

    CollectionTable.insert_one(
        creator_user_id=user_id,
        team_id=team_id,
        name=new_collection_name,
        display_name=collection.get('displayName'),
        description=description,
        logo=collection.get('logo'),
        embedding_model=embedding_model,
        dimension=dimension
    )
    return {
        "name": new_collection_name
    }


@app.delete("/api/vector/collections/<string:name>")
def delete_collection(name):
    team_id = request.team_id
    CollectionTable.delete_by_name(team_id, name)

    drop_milvus_collection(name)
    return {
        "success": True
    }
