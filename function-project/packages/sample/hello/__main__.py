import json
import os
import boxsdk

from urllib import parse


def auth_url(redirect_url: str):
    client_id = os.environ.get("CLIENT_ID")
    client_secret = os.environ.get("CLIENT_SECRET")
    box_oauth = boxsdk.OAuth2(client_id=client_id, client_secret=client_secret)
    client = boxsdk.Client(oauth=box_oauth)
    return json.dumps(client.oauth.get_authorization_url(redirect_url=redirect_url))


def main(event, context):
    print("let us begin")
    path = event["http"]["path"]
    if path in ["/auth_url", "/auth_url/"] and event["http"]["method"].upper() == "GET":
        redirect_url: str = parse.parse_qs(event["http"]["queryString"])[
            "redirect_url"
        ][0]
        return {
            "body": auth_url(redirect_url),
            "headers": {"Content-Type": "text/plain"},
        }

    return {
        "body": {
            "event": event,
            "context": {
                "activationId": context.activation_id,
                "apiHost": context.api_host,
                "apiKey": context.api_key,
                "deadline": context.deadline,
                "functionName": context.function_name,
                "functionVersion": context.function_version,
                "namespace": context.namespace,
                "requestId": context.request_id,
            },
        },
    }
