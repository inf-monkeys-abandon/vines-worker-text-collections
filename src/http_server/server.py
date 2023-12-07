from vines_worker_sdk.server import create_server
import os

JWT_SECRET = os.environ.get("JWT_SECRET")
if not JWT_SECRET:
    raise Exception("请在环境变量中配置 JWT_SECRET")

app = create_server(
    jwt_secret=JWT_SECRET,
    import_name="vines-worker-milvus",
)
