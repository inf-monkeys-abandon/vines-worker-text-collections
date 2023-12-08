from .server import app
from flask import request
from src.milvus import create_milvus_user
from src.database import AccountTable

from bson.json_util import dumps
from src.utils import generate_short_id, generate_random_string


@app.post("/api/vector/init-milvus-user")
def init_milvus_user():
    team_id = request.team_id
    role_name = f"team_{team_id}"
    username = f"vines_{generate_short_id()}"
    password = f"vector_{generate_random_string(32)}"

    account = AccountTable.find_by_team_id(team_id)
    exist = bool(account)
    if not exist:
        create_milvus_user(
            role_name,
            username,
            password
        )
        account = AccountTable.create_user(
            team_id=team_id,
            role_name=role_name,
            username=username,
            password=password
        )
    return dumps(account)
