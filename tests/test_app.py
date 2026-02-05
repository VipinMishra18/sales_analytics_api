import pytest

import app as app_module


@pytest.fixture(autouse=True)
def reset_state():
    app_module.transactions.clear()
    app_module.product_totals.clear()
    app_module.customer_totals.clear()
    app_module.product_index.clear()
    app_module.customer_index.clear()
    app_module.next_id = 1
    yield


@pytest.fixture()
def client():
    app_module.app.config.update({"TESTING": True})
    with app_module.app.test_client() as test_client:
        yield test_client


def test_root_endpoint(client):
    response = client.get("/")
    assert response.status_code == 200
    payload = response.get_json()
    assert payload["message"] == "Sales Analytics API"
    assert "transactions" in payload["endpoints"]


def test_create_and_get_transaction(client):
    payload = {
        "product_id": "P100",
        "product_name": "Premium Widget",
        "customer_id": "C42",
        "customer_name": "Alex",
        "quantity": 3,
        "price": 49.99,
        "timestamp": "2026-02-05T12:00:00+00:00",
    }
    create_response = client.post("/transactions", json=payload)
    assert create_response.status_code == 201
    tx = create_response.get_json()["transaction"]
    assert tx["id"] == 1
    assert tx["total"] == pytest.approx(149.97, rel=1e-3)

    get_response = client.get("/transactions/1")
    assert get_response.status_code == 200
    fetched = get_response.get_json()["transaction"]
    assert fetched["product_id"] == "P100"


def test_analytics_endpoints(client):
    client.post("/transactions", json={
        "product_id": "P1",
        "customer_id": "C1",
        "quantity": 2,
        "price": 10.0,
        "timestamp": "2026-02-05T12:00:00+00:00",
    })
    client.post("/transactions", json={
        "product_id": "P2",
        "customer_id": "C1",
        "quantity": 1,
        "price": 25.0,
        "timestamp": "2026-02-05T12:00:00+00:00",
    })

    totals_response = client.get("/analytics/total-sales-per-product")
    assert totals_response.status_code == 200
    totals = totals_response.get_json()["products"]
    assert totals[0]["total_sales"] == pytest.approx(25.0)

    top_response = client.get("/analytics/top-customers?limit=1")
    assert top_response.status_code == 200
    top = top_response.get_json()["customers"][0]
    assert top["customer_id"] == "C1"
    assert top["total_sales"] == pytest.approx(45.0)


def test_filters_and_pagination(client):
    client.post("/transactions", json={
        "product_id": "P1",
        "customer_id": "C1",
        "quantity": 1,
        "price": 5.0,
        "timestamp": "2026-02-05T12:00:00+00:00",
    })
    client.post("/transactions", json={
        "product_id": "P1",
        "customer_id": "C2",
        "quantity": 1,
        "price": 15.0,
        "timestamp": "2026-02-05T12:00:00+00:00",
    })
    client.post("/transactions", json={
        "product_id": "P2",
        "customer_id": "C2",
        "quantity": 1,
        "price": 25.0,
        "timestamp": "2026-02-05T12:00:00+00:00",
    })

    response = client.get("/transactions?product_id=P1&min_total=10")
    payload = response.get_json()
    assert payload["count"] == 1
    assert payload["transactions"][0]["customer_id"] == "C2"

    paged = client.get("/transactions?limit=1&offset=1")
    assert paged.get_json()["count"] == 3
    assert len(paged.get_json()["transactions"]) == 1
