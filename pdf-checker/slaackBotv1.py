import logging
import os
from slack_sdk import WebClient
from slack_sdk.errors import SlackApiError

client = WebClient(token="")
logger = logging.getLogger(__name__)

channel_name = "testowy-channel"
conversation_id = None
try:
    for result in client.conversations_list():
        if conversation_id is not None:
            break
        for channel in result["channels"]:
            if channel["name"] == channel_name:
                conversation_id = channel["id"]
                # Print result
                print(f"Found conversation ID: {conversation_id}")
                break

except SlackApiError as e:
    print(f"Error: {e}")
