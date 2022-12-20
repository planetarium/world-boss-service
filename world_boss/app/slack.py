from slack_sdk import WebClient

__all__ = ["client"]

from world_boss.app.config import config

token = config.slack_token

client = WebClient(token=token)
