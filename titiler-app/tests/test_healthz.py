"""The container's healthcheck asks a worker, not the port (#699).

The healthcheck in compose curls this route: an answer means a worker took the
request. It touches no storage, Redis or Django, so it does not go unhealthy
with them.
"""


def test_a_live_worker_answers_its_health_route(client):
    response = client.get("/healthz")

    assert response.status_code == 200
    assert response.headers["cache-control"] == "no-store"
