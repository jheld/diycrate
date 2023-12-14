import json
import os
import re
from typing import Optional, Union
import boxsdk
import traceback

from urllib import parse


def auth_url(redirect_url: str) -> str:
    client_id = os.environ.get("CLIENT_ID")
    client_secret = os.environ.get("CLIENT_SECRET")
    if not client_id:
        raise ValueError(f"Bad {client_id=}")
    box_oauth = boxsdk.OAuth2(client_id=client_id, client_secret=client_secret)
    auth_url_resp = box_oauth.get_authorization_url(redirect_url=redirect_url)
    return json.dumps(auth_url_resp)


def authenticate_url(code: str) -> str:
    """

    :return:
    """
    client_id = os.environ.get("CLIENT_ID")
    client_secret = os.environ.get("CLIENT_SECRET")
    if not client_id:
        raise ValueError(f"Bad {client_id=}")
    box_oauth = boxsdk.OAuth2(client_id=client_id, client_secret=client_secret)
    auth_code = code
    return json.dumps(
        [
            el.decode(encoding="utf-8", errors="strict")
            if isinstance(el, bytes)
            else el
            for el in box_oauth.authenticate(auth_code=auth_code)
        ]
    )


def new_access(
    access_token: Optional[str],
    refresh_token: str,
) -> list[Union[None, str]]:
    """
    Performs refresh of tokens and returns the result
    :return:
    """
    client_id = os.environ.get("CLIENT_ID")
    client_secret = os.environ.get("CLIENT_SECRET")
    if not client_id:
        raise ValueError(f"Bad {client_id=}")
    box_oauth = boxsdk.OAuth2(client_id=client_id, client_secret=client_secret)
    access_token_to_refresh = access_token or ""
    box_oauth._update_current_tokens(str(access_token_to_refresh), str(refresh_token))
    try:
        refresh_response: tuple[Optional[str], Optional[str]] = box_oauth.refresh(
            access_token_to_refresh
        )
        str_response = [
            el.decode(encoding="utf-8", errors="strict")
            if isinstance(el, bytes)
            else el
            for el in refresh_response
        ]
        # we've done the work, so let's wipe the temporary state adjustment clean
        box_oauth._update_current_tokens(None, None)
        return str_response
    except boxsdk.exception.BoxOAuthException as e:
        raise e


def main(event, _):
    path = event["http"]["path"]
    if re.match(r"^/auth_url(/)?", path) and event["http"]["method"] == "GET":
        query_string = event["http"]["queryString"]
        redirect_url: str = parse.parse_qs(query_string)["redirect_url"][0]
        status_code = 200
        try:
            response = auth_url(redirect_url)
        except Exception:
            response = traceback.format_exc()
            status_code = 500
        return {
            "body": response,
            "statusCode": status_code,
            "headers": {"Content-Type": "application/json"},
        }
    elif re.match(r"^/authenticate(/)?", path) and event["http"]["method"] == "POST":
        body: str = event["http"]["body"]
        status_code = 200

        try:
            response = authenticate_url(json.loads(body)["code"])
        except Exception:
            response = traceback.format_exc()
            status_code = 500
        return {
            "body": response,
            "statusCode": status_code,
            "headers": {"Content-Type": "application/json"},
        }

    elif re.match(r"/new_access(/)?", path) and event["http"]["method"] == "POST":
        body: str = event["http"]["body"]
        status_code = 200
        try:
            body_json = json.loads(body)
            response = new_access(body_json["access_token"], body_json["refresh_token"])
            response = json.dumps(response)
        except boxsdk.exception.BoxOAuthException as e:
            status_code = e.status
            response = json.dumps(e.message)

        except Exception:
            status_code = 500
            response = traceback.format_exc()
        return {
            "body": response,
            "statusCode": status_code,
            "headers": {"Content-Type": "application/json"},
        }

    return {
        "body": "Unsupported operation",
        "statusCode": 400,
        "headers": {"Content-Type": "application/json"},
    }
