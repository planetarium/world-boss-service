from flask.testing import FlaskClient


def test_ping(fx_test_client: FlaskClient):
    req = fx_test_client.get("/ping")
    assert req.status_code == 200
    assert req.data == b"pong"
