import json
import uuid
import logging
import boto3
from botocore.exceptions import ClientError

logger = logging.getLogger()
logger.setLevel(logging.INFO)

TABLE_NAME = "JoseGuzmanUsers"

dynamodb = boto3.resource("dynamodb")
table = dynamodb.Table(TABLE_NAME)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def response(status_code: int, body: dict) -> dict:
    return {
        "statusCode": status_code,
        "headers": {"Content-Type": "application/json"},
        "body": json.dumps(body),
    }


def parse_body(event: dict) -> dict:
    raw = event.get("body") or "{}"
    return json.loads(raw)


# ---------------------------------------------------------------------------
# CRUD actions
# ---------------------------------------------------------------------------

def create_user(event: dict) -> dict:
    """POST /users — create a new user."""
    body = parse_body(event)

    name = body.get("name", "").strip()
    email = body.get("email", "").strip()

    if not name or not email:
        return response(400, {"error": "Both 'name' and 'email' are required."})

    user_id = str(uuid.uuid4())

    item = {
        "userId": user_id,
        "name": name,
        "email": email,
    }

    try:
        table.put_item(
            Item=item,
            ConditionExpression="attribute_not_exists(userId)",
        )
    except ClientError as e:
        if e.response["Error"]["Code"] == "ConditionalCheckFailedException":
            return response(409, {"error": "User already exists."})
        logger.exception("DynamoDB put_item failed")
        return response(500, {"error": "Failed to create user."})

    logger.info("Created user %s", user_id)
    return response(201, {"message": "User created.", "user": item})


def get_user(event: dict) -> dict:
    """GET /users/{userId} — fetch a single user."""
    user_id = (event.get("pathParameters") or {}).get("userId")

    if not user_id:
        return response(400, {"error": "'userId' path parameter is required."})

    try:
        result = table.get_item(Key={"userId": user_id})
    except ClientError:
        logger.exception("DynamoDB get_item failed")
        return response(500, {"error": "Failed to retrieve user."})

    item = result.get("Item")
    if not item:
        return response(404, {"error": f"User '{user_id}' not found."})

    return response(200, {"user": item})


def update_user(event: dict) -> dict:
    """PUT /users/{userId} — update a user's name and email."""
    user_id = (event.get("pathParameters") or {}).get("userId")

    if not user_id:
        return response(400, {"error": "'userId' path parameter is required."})

    body = parse_body(event)
    name = body.get("name", "").strip()
    email = body.get("email", "").strip()

    if not name or not email:
        return response(400, {"error": "Both 'name' and 'email' are required."})

    try:
        result = table.update_item(
            Key={"userId": user_id},
            UpdateExpression="SET #n = :name, email = :email",
            ExpressionAttributeNames={"#n": "name"},
            ExpressionAttributeValues={":name": name, ":email": email},
            ConditionExpression="attribute_exists(userId)",
            ReturnValues="ALL_NEW",
        )
    except ClientError as e:
        if e.response["Error"]["Code"] == "ConditionalCheckFailedException":
            return response(404, {"error": f"User '{user_id}' not found."})
        logger.exception("DynamoDB update_item failed")
        return response(500, {"error": "Failed to update user."})

    logger.info("Updated user %s", user_id)
    return response(200, {"message": "User updated.", "user": result["Attributes"]})


def delete_user(event: dict) -> dict:
    """DELETE /users/{userId} — delete a user."""
    user_id = (event.get("pathParameters") or {}).get("userId")

    if not user_id:
        return response(400, {"error": "'userId' path parameter is required."})

    try:
        table.delete_item(
            Key={"userId": user_id},
            ConditionExpression="attribute_exists(userId)",
        )
    except ClientError as e:
        if e.response["Error"]["Code"] == "ConditionalCheckFailedException":
            return response(404, {"error": f"User '{user_id}' not found."})
        logger.exception("DynamoDB delete_item failed")
        return response(500, {"error": "Failed to delete user."})

    logger.info("Deleted user %s", user_id)
    return response(200, {"message": f"User '{user_id}' deleted."})


# ---------------------------------------------------------------------------
# Router
# ---------------------------------------------------------------------------

def resolve_route(method: str, path: str):
    """Match the incoming method + path to the right handler function."""
    if method == "POST" and path == "/users":
        return create_user
    elif method == "GET" and path.startswith("/users/"):
        return get_user
    elif method == "PUT" and path.startswith("/users/"):
        return update_user
    elif method == "DELETE" and path.startswith("/users/"):
        return delete_user
    else:
        return None


def handler(event: dict, context) -> dict:
    """Lambda entry point."""
    method = event.get("httpMethod", "")
    path = event.get("path", "")

    logger.info("Received %s %s", method, path)

    route_handler = resolve_route(method, path)

    if route_handler is None:
        return response(404, {"error": f"Route '{method} {path}' not found."})

    try:
        return route_handler(event)
    except Exception:
        logger.exception("Unhandled exception in route handler")
        return response(500, {"error": "Internal server error."})