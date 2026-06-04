import pytest

from lemur.domains.views import *  # noqa


from .vectors import (
    VALID_ADMIN_API_TOKEN,
    VALID_ADMIN_HEADER_TOKEN,
    VALID_READ_ONLY_HEADER_TOKEN,
    VALID_USER_HEADER_TOKEN,
)


@pytest.mark.parametrize(
    "token,status",
    [
        (VALID_USER_HEADER_TOKEN, 200),
        (VALID_ADMIN_HEADER_TOKEN, 200),
        (VALID_ADMIN_API_TOKEN, 200),
        ("", 401),
    ],
)
def test_domain_get(client, token, status):
    assert (
        client.get(api.url_for(Domains, domain_id=1), headers=token).status_code
        == status
    )


@pytest.mark.parametrize(
    "token,status",
    [
        (VALID_USER_HEADER_TOKEN, 405),
        (VALID_ADMIN_HEADER_TOKEN, 405),
        (VALID_ADMIN_API_TOKEN, 405),
        ("", 405),
    ],
)
def test_domain_post_(client, token, status):
    assert (
        client.post(
            api.url_for(Domains, domain_id=1), data={}, headers=token
        ).status_code
        == status
    )


@pytest.mark.parametrize(
    "token,status",
    [
        (VALID_USER_HEADER_TOKEN, 400),
        (VALID_ADMIN_HEADER_TOKEN, 400),
        (VALID_ADMIN_API_TOKEN, 400),
        ("", 401),
    ],
)
def test_domain_put(client, token, status):
    assert (
        client.put(
            api.url_for(Domains, domain_id=1), data={}, headers=token
        ).status_code
        == status
    )


@pytest.mark.parametrize(
    "token,status",
    [
        (VALID_USER_HEADER_TOKEN, 405),
        (VALID_ADMIN_HEADER_TOKEN, 405),
        (VALID_ADMIN_API_TOKEN, 405),
        ("", 405),
    ],
)
def test_domain_delete(client, token, status):
    assert (
        client.delete(api.url_for(Domains, domain_id=1), headers=token).status_code
        == status
    )


@pytest.mark.parametrize(
    "token,status",
    [
        (VALID_USER_HEADER_TOKEN, 405),
        (VALID_ADMIN_HEADER_TOKEN, 405),
        (VALID_ADMIN_API_TOKEN, 405),
        ("", 405),
    ],
)
def test_domain_patch(client, token, status):
    assert (
        client.patch(
            api.url_for(Domains, domain_id=1), data={}, headers=token
        ).status_code
        == status
    )


@pytest.mark.parametrize(
    "token,status",
    [
        (VALID_USER_HEADER_TOKEN, 400),
        (VALID_ADMIN_HEADER_TOKEN, 400),
        (VALID_ADMIN_API_TOKEN, 400),
        ("", 401),
    ],
)
def test_domain_list_post_(client, token, status):
    assert (
        client.post(api.url_for(DomainsList), data={}, headers=token).status_code
        == status
    )


@pytest.mark.parametrize(
    "token,status",
    [
        (VALID_USER_HEADER_TOKEN, 200),
        (VALID_ADMIN_HEADER_TOKEN, 200),
        (VALID_ADMIN_API_TOKEN, 200),
        ("", 401),
    ],
)
def test_domain_list_get(client, token, status):
    assert client.get(api.url_for(DomainsList), headers=token).status_code == status


@pytest.mark.parametrize(
    "token,status",
    [
        (VALID_USER_HEADER_TOKEN, 405),
        (VALID_ADMIN_HEADER_TOKEN, 405),
        (VALID_ADMIN_API_TOKEN, 405),
        ("", 405),
    ],
)
def test_domain_list_delete(client, token, status):
    assert client.delete(api.url_for(DomainsList), headers=token).status_code == status


@pytest.mark.parametrize(
    "token,status",
    [
        (VALID_USER_HEADER_TOKEN, 405),
        (VALID_ADMIN_HEADER_TOKEN, 405),
        (VALID_ADMIN_API_TOKEN, 405),
        ("", 405),
    ],
)
def test_domain_list_patch(client, token, status):
    assert (
        client.patch(api.url_for(DomainsList), data={}, headers=token).status_code
        == status
    )


def test_domain_create_read_only_forbidden(client):
    """Read-only users must be denied write access (GHSA-qcqw-jwxc-2hqg)."""
    import json
    resp = client.post(
        api.url_for(DomainsList),
        data=json.dumps({"name": "example.com", "sensitive": False}),
        headers=VALID_READ_ONLY_HEADER_TOKEN,
    )
    assert resp.status_code == 403


def test_domain_update_read_only_forbidden(client):
    """Read-only users must be denied write access (GHSA-qcqw-jwxc-2hqg)."""
    import json
    resp = client.put(
        api.url_for(Domains, domain_id=1),
        data=json.dumps({"name": "example.com", "sensitive": False}),
        headers=VALID_READ_ONLY_HEADER_TOKEN,
    )
    assert resp.status_code == 403
