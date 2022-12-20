from functools import wraps

from flask import abort, request
from slack_sdk import WebClient

__all__ = ["client", "slack_auth"]

from slack_sdk.signature import SignatureVerifier

from world_boss.app.config import config

token = config.slack_token
signing_secret = config.slack_signing_secret

client = WebClient(token=token)

verifier = SignatureVerifier(signing_secret=config.slack_signing_secret)


def slack_auth(f):
    @wraps(f)
    def func(*args, **kwargs):
        valid = verifier.is_valid_request(request.get_data(), request.headers)
        if not valid:
            abort(403)
        return f(*args, **kwargs)

    return func
