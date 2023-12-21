from .server import app
from flask import request
from src.milvus import create_milvus_user
from src.database import AccountTable

from bson.json_util import dumps
from src.utils import generate_short_id, generate_random_string


def init_milvus_user_if_not_exists(app_id, team_id):
    role_name = f"team_{team_id}"
    username = f"vines_{generate_short_id()}"
    password = f"vector_{generate_random_string(32)}"

    table = AccountTable(
        app_id=app_id
    )
    account = table.find_by_team_id(team_id)
    exist = bool(account)
    if not exist:
        # fix: role 数目有限制，先下掉
        # create_milvus_user(
        #     role_name,
        #     username,
        #     password
        # )
        account = table.create_user(
            team_id=team_id,
            role_name=role_name,
            username=username,
            password=password
        )

    return account


@app.post("/api/vector/init-milvus-user")
def init_milvus_user_api_hander():
    team_id = request.team_id
    app_id = request.app_id
    account = init_milvus_user_if_not_exists(app_id, team_id)
    return dumps(account)
