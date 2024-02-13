import os
import json
import requests
from logger_util import Logger

SLACK_WEBHOOK_URL = os.getenv("SLACK_WEBHOOK_URL", "")
IS_SLACK_NOTIFICATION_ENABLED = os.getenv("IS_SLACK_NOTIFICATION_ENABLED", "")


class NotificationUtil:
    def __init__(self) -> None:
        pass

    @staticmethod
    def notify_on_slack(message):
        try:
            if not IS_SLACK_NOTIFICATION_ENABLED == "ENABLED":
                return

            response = requests.post(SLACK_WEBHOOK_URL, json=message)
            Logger.info_log(f"Response on sending out slack notification {response}")

            if response.status_code == 200:
                return {
                    "statusCode": 200,
                    "body": json.dumps("Notification sent to Slack successfully"),
                }
            else:
                return {
                    "statusCode": response.status_code,
                    "body": json.dumps("Error sending notification to Slack"),
                }
        except Exception as e:
            Logger.error_log(f"Error in sending out slack notification {e}")

            return {
                "statusCode": 500,
                "body": json.dumps("Error sending notification to Slack"),
            }
