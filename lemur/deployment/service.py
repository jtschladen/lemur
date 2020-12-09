from lemur import database


def rotate_certificate(endpoint, new_cert):
    """
    Rotates a certificate on a given endpoint.

    :param endpoint:
    :param new_cert:
    :return:
    """
    old_cert = endpoint.certificate
    # ensure that certificate is available for rotation
    endpoint.source.plugin.update_endpoint(endpoint, new_cert)
    endpoint.certificate = new_cert
    database.update(endpoint)
    old_cert.notify = False
    database.update(old_cert)
