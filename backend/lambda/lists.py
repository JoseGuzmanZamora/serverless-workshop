import json
import uuid
import logging
import boto3
from boto3.dynamodb.conditions import Key
from botocore.exceptions import ClientError

logger = logging.getLogger()
logger.setLevel(logging.INFO)

TABLE_NAME = "JoseGuzmanLists"

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

def create_list(event: dict) -> dict:
    """POST /users/{userId}/lists — create a new list for a user."""
    user_id = (event.get("pathParameters") or {}).get("userId")
    body = parse_body(event)

    name = body.get("name", "").strip()
    description = body.get("description", "").strip()

    if not user_id:
        return response(400, {"error": "'userId' path parameter is required."})

    if not name:
        return response(400, {"error": "'name' is required."})

    list_id = str(uuid.uuid4())

    item = {
        "userId": user_id,
        "listId": list_id,
        "name": name,
        "description": description,
        "items": {},
    }

    try:
        table.put_item(Item=item)
    except ClientError:
        logger.exception("DynamoDB put_item failed")
        return response(500, {"error": "Failed to create list."})

    logger.info("Created list %s for user %s", list_id, user_id)
    return response(201, {"message": "List created.", "list": item})


def get_lists_for_user(event: dict) -> dict:
    """GET /users/{userId}/lists — fetch all lists for a user."""
    user_id = (event.get("pathParameters") or {}).get("userId")

    if not user_id:
        return response(400, {"error": "'userId' path parameter is required."})

    try:
        result = table.query(
            KeyConditionExpression=Key("userId").eq(user_id),
        )
    except ClientError:
        logger.exception("DynamoDB query failed")
        return response(500, {"error": "Failed to retrieve lists."})

    return response(200, {"lists": result.get("Items", [])})


def get_list(event: dict) -> dict:
    """GET /users/{userId}/lists/{listId} — fetch a single list."""
    user_id = (event.get("pathParameters") or {}).get("userId")
    list_id = (event.get("pathParameters") or {}).get("listId")

    if not user_id or not list_id:
        return response(400, {"error": "'userId' and 'listId' path parameters are required."})

    try:
        result = table.get_item(Key={"userId": user_id, "listId": list_id})
    except ClientError:
        logger.exception("DynamoDB get_item failed")
        return response(500, {"error": "Failed to retrieve list."})

    item = result.get("Item")
    if not item:
        return response(404, {"error": f"List '{list_id}' not found."})

    return response(200, {"list": item})


def update_list(event: dict) -> dict:
    """PUT /users/{userId}/lists/{listId} — update a list's name, description, and items."""
    user_id = (event.get("pathParameters") or {}).get("userId")
    list_id = (event.get("pathParameters") or {}).get("listId")

    if not user_id or not list_id:
        return response(400, {"error": "'userId' and 'listId' path parameters are required."})

    body = parse_body(event)
    name = body.get("name", "").strip()
    description = body.get("description", "").strip()
    items = body.get("items", {})

    if not name:
        return response(400, {"error": "'name' is required."})

    # Validate that items is a dict with the right structure:
    #   { "1": { "text": "Buy milk", "done": false }, ... }
    if not isinstance(items, dict):
        return response(400, {"error": "'items' must be an object."})

    for key, value in items.items():
        if not isinstance(value, dict):
            return response(400, {"error": f"Item '{key}' must be an object."})
        if "text" not in value or "done" not in value:
            return response(400, {"error": f"Item '{key}' must have 'text' and 'done'."})

    try:
        result = table.update_item(
            Key={"userId": user_id, "listId": list_id},
            UpdateExpression="SET #n = :name, description = :description, #i = :items",
            ExpressionAttributeNames={"#n": "name", "#i": "items"},
            ExpressionAttributeValues={
                ":name": name,
                ":description": description,
                ":items": items,
            },
            ConditionExpression="attribute_exists(listId)",
            ReturnValues="ALL_NEW",
        )
    except ClientError as e:
        if e.response["Error"]["Code"] == "ConditionalCheckFailedException":
            return response(404, {"error": f"List '{list_id}' not found."})
        logger.exception("DynamoDB update_item failed")
        return response(500, {"error": "Failed to update list."})

    logger.info("Updated list %s", list_id)
    return response(200, {"message": "List updated.", "list": result["Attributes"]})


def toggle_item(event: dict) -> dict:
    """PATCH /users/{userId}/lists/{listId}/items/{itemKey} — toggle an item's done status."""
    user_id = (event.get("pathParameters") or {}).get("userId")
    list_id = (event.get("pathParameters") or {}).get("listId")
    item_key = (event.get("pathParameters") or {}).get("itemKey")

    if not user_id or not list_id or not item_key:
        return response(400, {"error": "'userId', 'listId', and 'itemKey' path parameters are required."})

    # First, get the current list to read the item's done value
    try:
        result = table.get_item(Key={"userId": user_id, "listId": list_id})
    except ClientError:
        logger.exception("DynamoDB get_item failed")
        return response(500, {"error": "Failed to retrieve list."})

    item = result.get("Item")
    if not item:
        return response(404, {"error": f"List '{list_id}' not found."})

    items = item.get("items", {})
    if item_key not in items:
        return response(404, {"error": f"Item '{item_key}' not found in list."})

    # Flip the done value
    current_done = items[item_key]["done"]
    new_done = not current_done

    # Update just that item's done field in DynamoDB
    try:
        result = table.update_item(
            Key={"userId": user_id, "listId": list_id},
            UpdateExpression="SET #i.#key.done = :done",
            ExpressionAttributeNames={"#i": "items", "#key": item_key},
            ExpressionAttributeValues={":done": new_done},
            ReturnValues="ALL_NEW",
        )
    except ClientError:
        logger.exception("DynamoDB update_item failed")
        return response(500, {"error": "Failed to toggle item."})

    logger.info("Toggled item %s in list %s to %s", item_key, list_id, new_done)
    return response(200, {"message": "Item toggled.", "list": result["Attributes"]})


def delete_list(event: dict) -> dict:
    """DELETE /users/{userId}/lists/{listId} — delete a list."""
    user_id = (event.get("pathParameters") or {}).get("userId")
    list_id = (event.get("pathParameters") or {}).get("listId")

    if not user_id or not list_id:
        return response(400, {"error": "'userId' and 'listId' path parameters are required."})

    try:
        table.delete_item(
            Key={"userId": user_id, "listId": list_id},
            ConditionExpression="attribute_exists(listId)",
        )
    except ClientError as e:
        if e.response["Error"]["Code"] == "ConditionalCheckFailedException":
            return response(404, {"error": f"List '{list_id}' not found."})
        logger.exception("DynamoDB delete_item failed")
        return response(500, {"error": "Failed to delete list."})

    logger.info("Deleted list %s", list_id)
    return response(200, {"message": f"List '{list_id}' deleted."})


# ---------------------------------------------------------------------------
# Router
# ---------------------------------------------------------------------------

def resolve_route(method: str, path: str):
    """Match the incoming method + path to the right handler function."""
    # POST /users/{userId}/lists
    if method == "POST" and path.endswith("/lists"):
        return create_list
    # GET /users/{userId}/lists
    elif method == "GET" and path.endswith("/lists"):
        return get_lists_for_user
    # GET /users/{userId}/lists/{listId}
    elif method == "GET" and "/lists/" in path and "/items/" not in path:
        return get_list
    # PUT /users/{userId}/lists/{listId}
    elif method == "PUT" and "/lists/" in path:
        return update_list
    # PATCH /users/{userId}/lists/{listId}/items/{itemKey}
    elif method == "PATCH" and "/items/" in path:
        return toggle_item
    # DELETE /users/{userId}/lists/{listId}
    elif method == "DELETE" and "/lists/" in path:
        return delete_list
    else:
        return None


def lambda_handler(event: dict, context) -> dict:
    """Lambda entry point."""
    method = event.get("requestContext", {}).get("http", {}).get("method", "")
    path = event.get("rawPath", "")
    stage = event.get("requestContext", {}).get("stage", "")
    if stage and path.startswith(f"/{stage}"):
        path = path[len(f"/{stage}"):]

    logger.info("Received %s %s", method, path)

    route_handler = resolve_route(method, path)

    if route_handler is None:
        return response(404, {"error": f"Route '{method} {path}' not found."})

    try:
        return route_handler(event)
    except Exception:
        logger.exception("Unhandled exception in route handler")
        return response(500, {"error": "Internal server error."})
