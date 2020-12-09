"""
.. module: lemur.notifications.service
    :platform: Unix

    :copyright: (c) 2018 by Netflix Inc., see AUTHORS for more
    :license: Apache, see LICENSE for more details.

.. moduleauthor:: Kevin Glisson <kglisson@netflix.com>

"""
from flask import current_app

from lemur import database
from lemur.certificates.models import Certificate
from lemur.common.utils import truthiness
from lemur.notifications.models import Notification


def create_default_notifications(base_name, recipients, expiration_intervals=None):
    """
    Will create standard notifications for a given owner. By default, the notification types are set as follows:
     - Expiration: 30, 10 and 2 days (unless an alternate set of intervals is supplied)
     - Rotation success: disabled

    If standard notifications already exist these will be returned instead of new notifications.

    :param base_name:
    :param recipients:
    :param expiration_intervals:
    :return:
    """
    if not recipients:
        return []

    options = [
        {
            "name": "recipients",
            "type": "str",
            "required": True,
            "validation": r"^([\w+-.%]+@[\w-.]+\.[A-Za-z]{2,4},?)+$",
            "helpMessage": "Comma delimited list of email addresses",
            "value": ",".join(recipients),
        },
    ]

    if expiration_intervals is None:
        expiration_intervals = current_app.config.get(
            "LEMUR_DEFAULT_EXPIRATION_NOTIFICATION_INTERVALS", [30, 15, 2]
        )

    name = f"DEFAULT_{base_name}"
    existing = get_by_label(name)
    if existing:
        return existing

    return create(
        label=name,
        plugin_name=current_app.config.get(
            "LEMUR_DEFAULT_NOTIFICATION_PLUGIN", "email-notification"
        ),
        options=list(options),
        expiration_intervals=list(expiration_intervals),  # TODO format?
        description=f"Default notification for {name}",
        certificates=[],
    )


def create(label, plugin_name, options, expiration_intervals, description, certificates):
    """
    Creates a new notification.

    :param label: Notification label
    :param plugin_name:
    :param options:
    :param expiration_intervals:
    :param description:
    :param certificates:
    :rtype : Notification
    :return:
    """
    notification = Notification(
        label=label,
        options=options,
        expiration_intervals=expiration_intervals,
        plugin_name=plugin_name,
        description=description
    )
    notification.certificates = certificates
    return database.create(notification)


def update(notification_id, label, plugin_name, options, description, active, certificates):
    """
    Updates an existing notification.

    :param notification_id:
    :param label: Notification label
    :param plugin_name:
    :param options:
    :param description:
    :param active:
    :param certificates:
    :rtype : Notification
    :return:
    """
    notification = get(notification_id)

    notification.label = label
    notification.plugin_name = plugin_name
    notification.options = options
    notification.description = description
    notification.active = active
    notification.certificates = certificates

    return database.update(notification)


def delete(notification_id):
    """
    Deletes an notification.

    :param notification_id: Lemur assigned ID
    """
    database.delete(get(notification_id))


def get(notification_id):
    """
    Retrieves an notification by its lemur assigned ID.

    :param notification_id: Lemur assigned ID
    :rtype : Notification
    :return:
    """
    return database.get(Notification, notification_id)


def get_by_label(label):
    """
    Retrieves a notification by its label

    :param label:
    :return:
    """
    return database.get(Notification, label, field="label")


def get_all():
    """
    Retrieves all notification currently known by Lemur.

    :return:
    """
    query = database.session_query(Notification)
    return database.find_all(query, Notification, {}).all()


def render(args):
    filt = args.pop("filter")
    certificate_id = args.pop("certificate_id", None)

    if certificate_id:
        query = database.session_query(Notification).join(
            Certificate, Notification.certificate
        )
        query = query.filter(Certificate.id == certificate_id)
    else:
        query = database.session_query(Notification)

    if filt:
        terms = filt.split(";")
        if terms[0] == "active":
            query = query.filter(Notification.active == truthiness(terms[1]))
        else:
            query = database.filter(query, Notification, terms)

    return database.sort_and_page(query, Notification, args)
