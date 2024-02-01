from vines_worker_sdk.server import create_server
import os

JWT_SECRET = os.environ.get("JWT_SECRET")
if not JWT_SECRET:
    raise Exception("请在环境变量中配置 JWT_SECRET")

SERVICE_AUTHENTICATION_TOKEN = os.environ.get("SERVICE_AUTHENTICATION_TOKEN")

app = create_server(
    jwt_secret=JWT_SECRET,
    service_token=SERVICE_AUTHENTICATION_TOKEN,
    import_name="vines-worker-text-collection",
)
