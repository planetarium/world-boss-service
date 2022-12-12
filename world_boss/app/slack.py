import os

from slack_sdk import WebClient

__all__ = ["client"]

token = os.environ["SLACK_TOKEN"]

client = WebClient(token=token)
