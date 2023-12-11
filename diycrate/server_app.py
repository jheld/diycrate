import argparse
import configparser
import json
import logging
from pathlib import Path
import typing

from bottle import ServerAdapter
import boxsdk
from cheroot.wsgi import Server
from cheroot.ssl.builtin import BuiltinSSLAdapter
import cherrypy
from fastapi import Form, HTTPException
import uvicorn
from diycrate import utils

from diycrate.cache_utils import r_c
from diycrate.oauth_utils import setup_oauth, store_tokens_callback
from diycrate.log_utils import setup_logger


setup_logger()

crate_logger = logging.getLogger(__name__)


cloud_provider_name = "Box"

bottle_app = utils.Bottle()

app = utils.FastAPI()


# @bottle_app.route("/auth_url")
@app.get("/auth_url/")
def auth_url(redirect_url: str):
    """

    :return:
    """
    crate_logger.info("auth url retrieved.")

    app.oauth = setup_oauth(r_c, conf_obj, store_tokens_callback)
    # request = typing.cast(utils.LocalRequest, bottle.request)
    return json.dumps(app.oauth.get_authorization_url(redirect_url=redirect_url))


# @bottle_app.route("/authenticate", method="POST")
@app.post("/authenticate")
def authenticate_url(code: typing.Annotated[str, Form()]):
    """

    :return:
    """
    crate_logger.info("authentication flow initiated.")
    app.oauth = setup_oauth(r_c, conf_obj, store_tokens_callback)
    # request = typing.cast(utils.LocalRequest, bottle.request)
    # auth_code = request.POST.get("code")
    auth_code = code
    return json.dumps(
        [
            el.decode(encoding="utf-8", errors="strict")
            if isinstance(el, bytes)
            else el
            for el in app.oauth.authenticate(auth_code=auth_code)
        ]
    )


# @bottle_app.route("/new_access", method="POST")
@app.post("/new_access")
def new_access(
    access_token: typing.Annotated[typing.Optional[str], Form()],
    refresh_token: typing.Annotated[typing.Optional[str], Form()],
):
    """
    Performs refresh of tokens and returns the result
    :return:
    """
    crate_logger.info("generating an access token.")
    app.oauth = setup_oauth(r_c, conf_obj, store_tokens_callback)
    # request = typing.cast(utils.LocalRequest, bottle.request)
    # access_token_to_refresh = request.POST.get("access_token")
    access_token_to_refresh = access_token
    # refresh_token = request.POST.get("refresh_token")
    app.oauth._update_current_tokens(str(access_token_to_refresh), str(refresh_token))
    try:
        refresh_response = app.oauth.refresh(access_token_to_refresh)
        str_response = [
            el.decode(encoding="utf-8", errors="strict")
            if isinstance(el, bytes)
            else el
            for el in refresh_response
        ]
        # we've done the work, so let's wipe the temporary state adjustment clean
        app.oauth._update_current_tokens(None, None)
        return str_response
    except boxsdk.exception.BoxOAuthException as e:
        # we've done the work, so let's wipe the temporary state adjustment clean
        app.oauth._update_current_tokens(None, None)
        # response.status = e.status
        raise HTTPException(status_code=e.status, detail=json.dumps(e.message))


# Create our own sub-class of Bottle's ServerAdapter
# so that we can specify SSL. Using just server='cherrypy'
# uses the default cherrypy server, which doesn't use SSL
class SSLCherryPyServer(ServerAdapter):
    """
    Custom server adapter using cherry-py with ssl
    """

    def run(self, server_handler):
        """
        Overrides super to setup Cherry py with ssl and start the server.
        :param server_handler: originating server type
        :type server_handler:
        """
        server = Server((self.host, self.port), server_handler)
        # Uses the following github page's recommendation for setting up the cert:
        # https://github.com/nickbabcock/bottle-ssl
        server.ssl_adapter = BuiltinSSLAdapter(
            conf_obj["ssl"]["cacert_pem_path"],
            conf_obj["ssl"]["privkey_pem_path"],
            conf_obj["ssl"].get("chain_pem_path"),
        )
        try:
            server.start()
        finally:
            server.stop()

    @cherrypy.tools.register("before_finalize", priority=60)
    def secureheaders():
        headers = cherrypy.response.headers
        headers["X-Frame-Options"] = "DENY"
        headers["X-XSS-Protection"] = "1; mode=block"
        headers["Content-Security-Policy"] = "default-src 'self';"
        if (
            cherrypy.server.ssl_certificate is not None
            and cherrypy.server.ssl_private_key is not None
        ):
            headers["Strict-Transport-Security"] = "max-age=31536000"  # one year


conf_obj = configparser.ConfigParser()


def main():
    global conf_obj

    conf_dir = Path("~/.config/diycrate_server").expanduser().resolve()
    if not conf_dir.is_dir():
        conf_dir.mkdir()
    cloud_credentials_file_path = conf_dir / "box.ini"
    if not cloud_credentials_file_path.is_file():
        cloud_credentials_file_path.write_text("")
    conf_obj.read(cloud_credentials_file_path)
    args = get_arg_parser()

    had_oauth2 = conf_obj.has_section("oauth2")
    if not had_oauth2:
        conf_obj.add_section("oauth2")
    conf_obj["oauth2"] = {
        "client_id": args.client_id or conf_obj["oauth2"]["client_id"],
        "client_secret": args.client_secret or conf_obj["oauth2"]["client_secret"],
    }
    configure_ssl_conf(args, conf_obj)

    with cloud_credentials_file_path.open("w") as fh:
        conf_obj.write(fh)
    uvicorn.run(
        app=app,
        port=args.port,
        host="0.0.0.0",
        ssl_certfile=conf_obj["ssl"]["cacert_pem_path"],
        ssl_keyfile=conf_obj["ssl"]["privkey_pem_path"],
        ssl_ca_certs=conf_obj["ssl"].get("chain_pem_path"),
    )
    # bottle_app.run(server=SSLCherryPyServer, port=args.port, host="0.0.0.0")


def configure_ssl_conf(args, conf_obj):
    try:
        prev_chain_pem_path = conf_obj["ssl"]["chain_pem_path"]
    except KeyError:
        prev_chain_pem_path = None

    if "ssl" not in conf_obj:
        if not args.cacert_pem_path:
            raise ValueError("Need a valid cacert_pem_path")
        if not args.privkey_pem_path:
            raise ValueError("Need a valid privkey_pem_path")
        conf_obj["ssl"] = {
            "cacert_pem_path": Path(args.cacert_pem_path).expanduser().absolute(),
            "privkey_pem_path": Path(args.privkey_pem_path).expanduser().absolute(),
        }
        if args.chain_pem_path:
            conf_obj["ssl"]["chain_pem_path"] = (
                Path(args.chain_pem_path).expanduser().resolve()
            )

    conf_obj["ssl"] = {
        "cacert_pem_path": Path(args.cacert_pem_path).expanduser().resolve()
        if args.cacert_pem_path
        else conf_obj["ssl"]["cacert_pem_path"],
        "privkey_pem_path": Path(args.privkey_pem_path).expanduser().resolve()
        if args.privkey_pem_path
        else conf_obj["ssl"]["privkey_pem_path"],
    }

    if args.chain_pem_path or prev_chain_pem_path:
        conf_obj["ssl"]["chain_pem_path"] = (
            prev_chain_pem_path or Path(args.chain_pem_path).expanduser().resolve()
        )


def get_arg_parser():
    arg_parser = argparse.ArgumentParser()
    arg_parser.add_argument(
        "--client_id",
        type=str,
        help="Client ID provided by {}".format(cloud_provider_name),
        default="",
    )
    arg_parser.add_argument(
        "--client_secret",
        type=str,
        help="Client Secret provided by {}".format(cloud_provider_name),
        default="",
    )
    arg_parser.add_argument(
        "--cacert_pem_path",
        type=str,
        help="filepath to where the cacert.pem is located",
        default="",
    )
    arg_parser.add_argument(
        "--privkey_pem_path",
        type=str,
        help="filepath to where the privkey.pem is located",
        default="",
    )
    arg_parser.add_argument(
        "--chain_pem_path",
        type=str,
        help="filepath to where the chain.pem is located",
        default="",
    )
    arg_parser.add_argument("--port", type=int, help="server port", default=8081)
    args = arg_parser.parse_args()
    return args


if __name__ == "__main__":
    main()
