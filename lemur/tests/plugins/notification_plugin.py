from lemur.plugins.bases import NotificationPlugin
from lemur.plugins.utils import get_plugin_option


class TestNotificationPlugin(NotificationPlugin):
    title = "Test"
    slug = "test-notification"
    description = "Enables testing"

    author = "Kevin Glisson"
    author_url = "https://github.com/netflix/lemur.git"

    def __init__(self, *args, **kwargs):
        super(TestNotificationPlugin, self).__init__(*args, **kwargs)

    @staticmethod
    def send(notification_type, message, targets, options, **kwargs):
        return

    @staticmethod
    def get_recipients(options, additional_recipients, **kwargs):
        notification_recipients = get_plugin_option("recipients", options)
        if notification_recipients:
            notification_recipients = notification_recipients.split(",")

        return list(set(notification_recipients + additional_recipients))
