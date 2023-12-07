from .server import app
from flask import request
from src.milvus import create_milvus_user
from src.database import AccountTable
from shortid import ShortId
from random import choice
from string import ascii_letters
from bson.json_util import dumps

sid = ShortId()


def generate_random_password():
    return ''.join(choice(ascii_letters) for i in range(12))


@app.post("/api/vector/init-milvus-user")
def init_milvus_user():
    team_id = request.team_id
    role_name = f"team_{team_id}"
    username = f"vines_{sid.generate()}"
    password = f"vector_{generate_random_password()}"

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
