import bottle
from boxsdk import OAuth2


class Bottle(bottle.Bottle):
    oauth: OAuth2
    csrf_token: str
