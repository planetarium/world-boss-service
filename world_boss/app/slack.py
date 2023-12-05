from functools import wraps

from fastapi import HTTPException
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
    async def func(*args, **kwargs):
        request = kwargs["request"]
        data = await request.body()
        valid = verifier.is_valid_request(data, request.headers)
        if not valid:
            raise HTTPException(403)
        return await f(*args, **kwargs)

    return func
