"""
.. module: lemur.plugins.lemur_email.plugin
    :platform: Unix
    :copyright: (c) 2018 by Netflix Inc., see AUTHORS for more
    :license: Apache, see LICENSE for more details.

.. moduleauthor:: Kevin Glisson <kglisson@netflix.com>
"""
import boto3
from flask import current_app
from flask_mail import Message

from lemur.extensions import smtp_mail
from lemur.exceptions import InvalidConfiguration

from lemur.plugins.bases import ExpirationNotificationPlugin
from lemur.plugins import lemur_email as email

from lemur.plugins.lemur_email.templates.config import env
from lemur.plugins.utils import get_plugin_option


def render_html(template_name, options, certificates):
    """
    Renders the html for our email notification.

    :param template_name:
    :param options:
    :param certificates:
    :return:
    """
    message = {"options": options, "certificates": certificates}
    template = env.get_template("{}.html".format(template_name))
    return template.render(
        dict(message=message, hostname=current_app.config.get("LEMUR_HOSTNAME"))
    )


def send_via_smtp(subject, body, targets):
    """
    Attempts to deliver email notification via SMTP.

    :param subject:
    :param body:
    :param targets:
    :return:
    """
    msg = Message(
        subject, recipients=targets, sender=current_app.config.get("LEMUR_EMAIL")
    )
    msg.body = ""  # kinda a weird api for sending html emails
    msg.html = body
    smtp_mail.send(msg)


def send_via_ses(subject, body, targets):
    """
    Attempts to deliver email notification via SES service.
    :param subject:
    :param body:
    :param targets:
    :return:
    """
    ses_region = current_app.config.get("LEMUR_SES_REGION", "us-east-1")
    client = boto3.client("ses", region_name=ses_region)
    source_arn = current_app.config.get("LEMUR_SES_SOURCE_ARN")
    args = {
        "Source": current_app.config.get("LEMUR_EMAIL"),
        "Destination": {"ToAddresses": targets},
        "Message": {
            "Subject": {"Data": subject, "Charset": "UTF-8"},
            "Body": {"Html": {"Data": body, "Charset": "UTF-8"}},
        },
    }
    if source_arn:
        args["SourceArn"] = source_arn
    client.send_email(**args)


class EmailNotificationPlugin(ExpirationNotificationPlugin):
    title = "Email"
    slug = "email-notification"
    description = "Sends expiration email notifications"
    version = email.VERSION

    author = "Kevin Glisson"
    author_url = "https://github.com/netflix/lemur"

    additional_options = [
        {
            "name": "recipients",
            "type": "str",
            "required": True,
            "validation": r"^([\w+-.%]+@[\w-.]+\.[A-Za-z]{2,4},?)+$",
            "helpMessage": "Comma delimited list of email addresses",
        }
    ]

    def __init__(self, *args, **kwargs):
        """Initialize the plugin with the appropriate details."""
        sender = current_app.config.get("LEMUR_EMAIL_SENDER", "ses").lower()

        if sender not in ["ses", "smtp"]:
            raise InvalidConfiguration("Email sender type {0} is not recognized.")

    @staticmethod
    def send(notification_type, message, targets, options, **kwargs):

        subject = "Lemur: {0} Notification".format(notification_type.capitalize())

        body = render_html(notification_type, options, message)

        s_type = current_app.config.get("LEMUR_EMAIL_SENDER", "ses").lower()

        current_app.logger.info(f"Sending email to targets {targets}")  # TODO remove

        if s_type == "ses":
            send_via_ses(subject, body, targets)

        elif s_type == "smtp":
            send_via_smtp(subject, body, targets)

    @staticmethod
    def filter_recipients(options, excluded_recipients, **kwargs):
        notification_recipients = get_plugin_option("recipients", options)
        if notification_recipients:
            notification_recipients = notification_recipients.split(",")
            # removing owner and security_email from notification_recipient
            notification_recipients = [i for i in notification_recipients if i not in excluded_recipients]

        return notification_recipients
