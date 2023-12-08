import time
import os

from pymongo import MongoClient

MONGO_COLLECTION_PREFIX = os.environ.get("MONGO_COLLECTION_PREFIX") or ""
MONGO_URL = os.environ.get("MONGO_URL")

client = MongoClient(MONGO_URL)
db = client.vines
COLLECTION_ENTITY = db[MONGO_COLLECTION_PREFIX + "vector-collections"]
ACCOUNT_ENTITY = db[MONGO_COLLECTION_PREFIX + "vector-accounts"]
FILE_PROCESS_PROGRESS_ENTITY = db[MONGO_COLLECTION_PREFIX + "vector-file-process-progress"]


class CollectionTable:
    @staticmethod
    def insert_one(
            creator_user_id,
            team_id,
            name,
            display_name,
            description,
            logo,
            embedding_model,
            dimension
    ):
        timestamp = int(time.time())
        return COLLECTION_ENTITY.insert_one({
            "createdTimestamp": timestamp,
            "updatedTimestamp": timestamp,
            "isDeleted": False,
            "creatorUserId": creator_user_id,
            "teamId": team_id,
            "name": name,
            "displayName": display_name,
            "description": description,
            "logo": logo,
            "embeddingModel": embedding_model,
            "dimension": dimension,
            "metadataFields": [
                {
                    "name": 'userId',
                    "displayName": '创建此向量的用户 ID',
                    "description": '创建此向量的用户 ID',
                    "builtIn": True,
                    "required": True,
                },
                {
                    "name": 'workflowId',
                    "displayName": '工作流 ID',
                    "description": '当此向量是通过工作流创建的时候会包含，为创建此向量的工作流 ID',
                    "builtIn": True,
                    "required": False,
                },
                {
                    "name": 'createdAt',
                    "displayName": '创建时间',
                    "description": 'Unix 时间戳',
                    "builtIn": True,
                    "required": True,
                },
                {
                    "name": 'fileUrl',
                    "displayName": '来源文件链接',
                    "description": '当通过文件导入时会包含此值，为来源文件的链接',
                    "builtIn": True,
                    "required": False,
                },
            ],
            "authorizedTargets": []
        })

    @staticmethod
    def check_name_conflicts(name):
        record = COLLECTION_ENTITY.find_one({
            "name": name,
            "isDeleted": False
        })
        return bool(record)

    @staticmethod
    def find_by_team(team_id):
        return COLLECTION_ENTITY.find({
            "$or": [
                {
                    "teamId": team_id,
                    "isDeleted": False,
                },
                {
                    "authorizedTargets": {
                        "$elemMatch": {
                            "targetType": "TEAM",
                            "targetId": team_id
                        }
                    }
                }
            ]
        }).sort("_id", -1)

    @staticmethod
    def find_by_name(team_id, name):
        return COLLECTION_ENTITY.find_one({
            "$or": [
                {
                    "isDeleted": False,
                    "name": name,
                    "teamId": team_id,
                },
                {
                    "isDeleted": False,
                    "name": name,
                    "authorizedTargets": {
                        "$elemMatch": {
                            "targetType": "TEAM",
                            "targetId": team_id
                        }
                    }
                }
            ]
        })

    @staticmethod
    def find_by_name_without_team(name):
        return COLLECTION_ENTITY.find_one({
            "isDeleted": False,
            "name": name
        })

    @staticmethod
    def update_by_name(team_id, name, description=None, display_name=None, logo=None):
        updates = {}
        if description:
            updates['description'] = description
        if display_name:
            updates['displayName'] = display_name
        if logo:
            updates['logo'] = logo
        updates['updatedTimestamp'] = int(time.time())
        return COLLECTION_ENTITY.update_one(
            {
                "teamId": team_id,
                "isDeleted": False,
                "name": name
            },
            {
                "$set": updates
            }
        )

    @staticmethod
    def delete_by_name(team_id, name):
        return COLLECTION_ENTITY.update_one(
            {
                "teamId": team_id,
                "isDeleted": False,
                "name": name
            },
            {
                "$set": {
                    "isDeleted": True
                }
            }
        )

    @staticmethod
    def authorize_target(name: str, team_id: str):
        return COLLECTION_ENTITY.update_one(
            {
                "isDeleted": False,
                "name": name
            },
            {
                "$push": {
                    "authorizedTargets": {
                        "targetType": "TEAM",
                        "targetId": team_id
                    }
                }
            }
        )

    @staticmethod
    def add_metadata_fields_if_not_exists(team_id, coll_name, field_names):
        coll = CollectionTable.find_by_name(team_id, coll_name)
        fields_to_add = []
        for field_name in field_names:
            not_exist = len(list(filter(lambda x: x['name'] == field_name, coll['metadataFields']))) == 0
            if not_exist:
                fields_to_add.append(field_name)
        if len(fields_to_add) == 0:
            return
        for field_name in fields_to_add:
            COLLECTION_ENTITY.update_one(
                {
                    "teamId": team_id,
                    "isDeleted": False,
                    "name": coll_name
                },
                {
                    "$push": {
                        "metadataFields": {
                            "name": field_name,
                            "displayName": '',
                            "description": '',
                            "builtIn": False,
                            "required": False,
                        }
                    }
                }
            )


class AccountTable:

    @staticmethod
    def find_by_team_id(team_id):
        return ACCOUNT_ENTITY.find_one({
            "teamId": team_id,
            "isDeleted": False,
        })

    @staticmethod
    def create_user(team_id, role_name, username, password):
        timestamp = int(time.time())
        ACCOUNT_ENTITY.insert_one({
            "createdTimestamp": timestamp,
            "updatedTimestamp": timestamp,
            "isDeleted": False,
            "teamId": team_id,
            "roleName": role_name,
            "username": username,
            "password": password
        })
        return AccountTable.find_by_team_id(team_id)

    @staticmethod
    def find_or_create(team_id, role_name, username, password):
        entity = AccountTable.find_by_team_id(team_id)
        if entity:
            return entity
        return AccountTable.create_user(team_id, role_name, username, password)


class FileProcessProgressTable:

    @staticmethod
    def list_tasks(team_id, collection_name):
        return FILE_PROCESS_PROGRESS_ENTITY.find(
            {
                "teamId": team_id,
                "collectionName": collection_name
            }
        ).sort("_id", -1)

    @staticmethod
    def get_task(team_id, collection_name, task_id):
        return FILE_PROCESS_PROGRESS_ENTITY.find_one({
            "teamId": team_id,
            "collectionName": collection_name,
            "taskId": task_id,
            "isDeleted": False,
        })

    @staticmethod
    def create_task(team_id, collection_name, task_id):
        timestamp = int(time.time())
        FILE_PROCESS_PROGRESS_ENTITY.insert_one({
            "createdTimestamp": timestamp,
            "updatedTimestamp": timestamp,
            "isDeleted": False,
            "teamId": team_id,
            "collectionName": collection_name,
            "taskId": task_id,
            "events": []
        })

    @staticmethod
    def mark_task_failed(task_id, message):
        FILE_PROCESS_PROGRESS_ENTITY.update_one(
            {
                "taskId": task_id
            },
            {
                "$push": {
                    "events": {
                        "message": message,
                        "timestamp": int(time.time()),
                        "status": "FAILED"
                    }
                }
            }
        )

    @staticmethod
    def update_progress(task_id, progress, message):
        status = "INPROGRESS" if progress < 1 else "COMPLETED"
        FILE_PROCESS_PROGRESS_ENTITY.update_one(
            {
                "taskId": task_id
            },
            {
                "$push": {
                    "events": {
                        "progress": progress,
                        "message": message,
                        "timestamp": int(time.time()),
                        "status": status
                    }
                }
            }
        )
