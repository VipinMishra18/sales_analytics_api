from __future__ import annotations

import heapq
import random
import time
from datetime import datetime, timezone
from typing import Any

from flask import Flask, jsonify, request

app = Flask(__name__)

# In-memory storage optimized for O(1) CRUD lookups by id.
transactions: dict[int, dict[str, Any]] = {}
# Pre-aggregated totals to answer analytics in O(n_products) / O(k log n).
product_totals: dict[str, float] = {}
customer_totals: dict[str, float] = {}
# Inverted indexes to reduce filter scans to candidate sets.
product_index: dict[str, set[int]] = {}
customer_index: dict[str, set[int]] = {}
next_id = 1


@app.route("/", methods=["GET"])
def index():
    return jsonify({
        "message": "Sales Analytics API",
        "endpoints": {
            "transactions": "/transactions",
            "transaction_by_id": "/transactions/<id>",
            "total_sales_per_product": "/analytics/total-sales-per-product",
            "top_customers": "/analytics/top-customers",
            "benchmark": "/analytics/benchmark",
        },
    })


def _parse_timestamp(value: str | None) -> datetime:
    """Parse ISO-8601 timestamps; default to now (UTC) when absent."""
    if not value:
        return datetime.now(timezone.utc)
    try:
        parsed = datetime.fromisoformat(value)
    except ValueError as exc:
        raise ValueError("timestamp must be ISO 8601") from exc
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _transaction_total(tx: dict[str, Any]) -> float:
    """Compute total value for a transaction (quantity * price)."""
    return float(tx["quantity"]) * float(tx["price"])


def _serialize_transaction(tx: dict[str, Any]) -> dict[str, Any]:
    """Return a JSON-friendly view with ISO timestamp and computed total."""
    return {
        **tx,
        "timestamp": tx["timestamp"].isoformat(),
        "total": round(_transaction_total(tx), 2),
    }


def _validate_payload(data: dict[str, Any], *, partial: bool = False) -> tuple[dict[str, Any], list[str]]:
    """Validate payload fields and normalize types; supports partial updates."""
    errors: list[str] = []
    normalized: dict[str, Any] = {}

    def require_field(field: str) -> bool:
        if field in data:
            return True
        if not partial:
            errors.append(f"{field} is required")
        return False

    if require_field("product_id"):
        value = str(data.get("product_id")).strip()
        if not value:
            errors.append("product_id cannot be empty")
        else:
            normalized["product_id"] = value

    if "product_name" in data:
        normalized["product_name"] = str(data.get("product_name", "")).strip() or None

    if require_field("customer_id"):
        value = str(data.get("customer_id")).strip()
        if not value:
            errors.append("customer_id cannot be empty")
        else:
            normalized["customer_id"] = value

    if "customer_name" in data:
        normalized["customer_name"] = str(data.get("customer_name", "")).strip() or None

    if require_field("quantity"):
        try:
            quantity = int(data.get("quantity"))
            if quantity <= 0:
                raise ValueError
            normalized["quantity"] = quantity
        except (TypeError, ValueError):
            errors.append("quantity must be a positive integer")

    if require_field("price"):
        try:
            price = float(data.get("price"))
            if price < 0:
                raise ValueError
            normalized["price"] = price
        except (TypeError, ValueError):
            errors.append("price must be a non-negative number")

    if "timestamp" in data:
        try:
            normalized["timestamp"] = _parse_timestamp(data.get("timestamp"))
        except ValueError as exc:
            errors.append(str(exc))

    return normalized, errors


def _index_transaction(tx_id: int, tx: dict[str, Any]) -> None:
    """Update aggregates and indexes on insert/update."""
    product_id = tx["product_id"]
    customer_id = tx["customer_id"]
    total = _transaction_total(tx)

    product_index.setdefault(product_id, set()).add(tx_id)
    customer_index.setdefault(customer_id, set()).add(tx_id)
    product_totals[product_id] = product_totals.get(product_id, 0.0) + total
    customer_totals[customer_id] = customer_totals.get(customer_id, 0.0) + total


def _unindex_transaction(tx_id: int, tx: dict[str, Any]) -> None:
    """Reverse aggregates and indexes on delete/update."""
    product_id = tx["product_id"]
    customer_id = tx["customer_id"]
    total = _transaction_total(tx)

    if product_id in product_index:
        product_index[product_id].discard(tx_id)
        if not product_index[product_id]:
            product_index.pop(product_id, None)
    if customer_id in customer_index:
        customer_index[customer_id].discard(tx_id)
        if not customer_index[customer_id]:
            customer_index.pop(customer_id, None)

    product_totals[product_id] = product_totals.get(product_id, 0.0) - total
    if product_totals[product_id] <= 0:
        product_totals.pop(product_id, None)
    customer_totals[customer_id] = customer_totals.get(customer_id, 0.0) - total
    if customer_totals[customer_id] <= 0:
        customer_totals.pop(customer_id, None)


def _apply_filters(
    product_id: str | None,
    customer_id: str | None,
    start_date: datetime | None,
    end_date: datetime | None,
    min_total: float | None,
    max_total: float | None,
) -> list[dict[str, Any]]:
    """Filter using indexes first to avoid full scans when possible."""
    candidate_ids: set[int] | None = None

    if product_id:
        candidate_ids = set(product_index.get(product_id, set()))
    if customer_id:
        customer_set = customer_index.get(customer_id, set())
        candidate_ids = set(customer_set) if candidate_ids is None else candidate_ids & customer_set

    if candidate_ids is None:
        candidate_iter = transactions.values()
    else:
        candidate_iter = (transactions[tx_id] for tx_id in candidate_ids)

    results: list[dict[str, Any]] = []
    for tx in candidate_iter:
        timestamp = tx["timestamp"]
        if start_date and timestamp < start_date:
            continue
        if end_date and timestamp > end_date:
            continue

        total = _transaction_total(tx)
        if min_total is not None and total < min_total:
            continue
        if max_total is not None and total > max_total:
            continue

        results.append(tx)

    return results


@app.route("/transactions", methods=["POST"])
def create_transaction():
    """Create a new transaction and update aggregates/indexes in O(1)."""
    data = request.get_json(silent=True) or {}
    normalized, errors = _validate_payload(data)
    if errors:
        return jsonify({"errors": errors}), 400

    global next_id
    tx_id = next_id
    next_id += 1

    tx = {
        "id": tx_id,
        "product_id": normalized["product_id"],
        "product_name": normalized.get("product_name"),
        "customer_id": normalized["customer_id"],
        "customer_name": normalized.get("customer_name"),
        "quantity": normalized["quantity"],
        "price": normalized["price"],
        "timestamp": normalized.get("timestamp") or datetime.now(timezone.utc),
    }

    transactions[tx_id] = tx
    _index_transaction(tx_id, tx)
    return jsonify({"transaction": _serialize_transaction(tx)}), 201


@app.route("/transactions", methods=["GET"])
def list_transactions():
    """List transactions with optional indexed filters and pagination."""
    product_id = request.args.get("product_id")
    customer_id = request.args.get("customer_id")

    start_date = request.args.get("start_date")
    end_date = request.args.get("end_date")
    min_total = request.args.get("min_total")
    max_total = request.args.get("max_total")

    try:
        start_dt = _parse_timestamp(start_date) if start_date else None
        end_dt = _parse_timestamp(end_date) if end_date else None
    except ValueError as exc:
        return jsonify({"errors": [str(exc)]}), 400

    try:
        min_total_val = float(min_total) if min_total is not None else None
        max_total_val = float(max_total) if max_total is not None else None
    except ValueError:
        return jsonify({"errors": ["min_total/max_total must be numbers"]}), 400

    results = _apply_filters(product_id, customer_id, start_dt, end_dt, min_total_val, max_total_val)

    limit = request.args.get("limit", type=int)
    offset = request.args.get("offset", type=int, default=0)
    if offset < 0:
        offset = 0
    if limit is not None and limit < 0:
        limit = None

    sliced = results[offset : offset + limit if limit is not None else None]
    return jsonify({
        "count": len(results),
        "transactions": [_serialize_transaction(tx) for tx in sliced],
    })


@app.route("/transactions/<int:tx_id>", methods=["GET"])
def get_transaction(tx_id: int):
    """Fetch a single transaction by id."""
    tx = transactions.get(tx_id)
    if not tx:
        return jsonify({"message": "Transaction not found"}), 404
    return jsonify({"transaction": _serialize_transaction(tx)})


@app.route("/transactions/<int:tx_id>", methods=["PUT"])
def update_transaction(tx_id: int):
    """Update a transaction and recompute aggregates/indexes."""
    tx = transactions.get(tx_id)
    if not tx:
        return jsonify({"message": "Transaction not found"}), 404

    data = request.get_json(silent=True) or {}
    normalized, errors = _validate_payload(data, partial=True)
    if errors:
        return jsonify({"errors": errors}), 400

    _unindex_transaction(tx_id, tx)

    tx.update({
        "product_id": normalized.get("product_id", tx["product_id"]),
        "product_name": normalized.get("product_name", tx.get("product_name")),
        "customer_id": normalized.get("customer_id", tx["customer_id"]),
        "customer_name": normalized.get("customer_name", tx.get("customer_name")),
        "quantity": normalized.get("quantity", tx["quantity"]),
        "price": normalized.get("price", tx["price"]),
        "timestamp": normalized.get("timestamp", tx["timestamp"]),
    })

    _index_transaction(tx_id, tx)
    return jsonify({"transaction": _serialize_transaction(tx)})


@app.route("/transactions/<int:tx_id>", methods=["DELETE"])
def delete_transaction(tx_id: int):
    """Delete a transaction and roll back aggregates/indexes."""
    tx = transactions.pop(tx_id, None)
    if not tx:
        return jsonify({"message": "Transaction not found"}), 404
    _unindex_transaction(tx_id, tx)
    return jsonify({"message": "Transaction deleted", "transaction": _serialize_transaction(tx)})


@app.route("/analytics/total-sales-per-product", methods=["GET"])
def total_sales_per_product():
    """Return product totals using pre-aggregated data."""
    results = [
        {"product_id": product_id, "total_sales": round(total, 2)}
        for product_id, total in product_totals.items()
    ]
    results.sort(key=lambda item: item["total_sales"], reverse=True)
    return jsonify({"products": results, "count": len(results)})


@app.route("/analytics/top-customers", methods=["GET"])
def top_customers():
    """Return top customers using a heap for O(n log k)."""
    limit = request.args.get("limit", type=int, default=10)
    if limit <= 0:
        return jsonify({"errors": ["limit must be positive"]}), 400

    top = heapq.nlargest(limit, customer_totals.items(), key=lambda item: item[1])
    results = [
        {"customer_id": customer_id, "total_sales": round(total, 2)}
        for customer_id, total in top
    ]
    return jsonify({"customers": results, "count": len(results)})


def run_benchmark(record_count: int = 50000, top_n: int = 10, query_rounds: int = 5) -> dict[str, float]:
    """Compare naive re-scan vs pre-aggregated query performance."""
    random.seed(42)
    products = [f"P{idx}" for idx in range(100)]
    customers = [f"C{idx}" for idx in range(1000)]
    dataset = [
        {
            "product_id": random.choice(products),
            "customer_id": random.choice(customers),
            "quantity": random.randint(1, 5),
            "price": round(random.uniform(5, 250), 2),
            "timestamp": datetime.now(timezone.utc),
        }
        for _ in range(record_count)
    ]

    start = time.perf_counter()
    for _ in range(query_rounds):
        naive_product_totals: dict[str, float] = {}
        naive_customer_totals: dict[str, float] = {}
        for tx in dataset:
            total = _transaction_total(tx)
            naive_product_totals[tx["product_id"]] = naive_product_totals.get(tx["product_id"], 0.0) + total
            naive_customer_totals[tx["customer_id"]] = naive_customer_totals.get(tx["customer_id"], 0.0) + total
        heapq.nlargest(top_n, naive_customer_totals.items(), key=lambda item: item[1])
    naive_time = time.perf_counter() - start

    start = time.perf_counter()
    opt_product_totals: dict[str, float] = {}
    opt_customer_totals: dict[str, float] = {}
    for tx in dataset:
        total = _transaction_total(tx)
        opt_product_totals[tx["product_id"]] = opt_product_totals.get(tx["product_id"], 0.0) + total
        opt_customer_totals[tx["customer_id"]] = opt_customer_totals.get(tx["customer_id"], 0.0) + total
    for _ in range(query_rounds):
        heapq.nlargest(top_n, opt_customer_totals.items(), key=lambda item: item[1])
    optimized_time = time.perf_counter() - start

    return {
        "records": record_count,
        "query_rounds": query_rounds,
        "naive_seconds": round(naive_time, 6),
        "optimized_seconds": round(optimized_time, 6),
    }


@app.route("/analytics/benchmark", methods=["GET"])
def benchmark_endpoint():
    """Expose benchmark results for quick validation."""
    record_count = request.args.get("records", type=int, default=50000)
    query_rounds = request.args.get("rounds", type=int, default=5)
    if record_count <= 0:
        return jsonify({"errors": ["records must be positive"]}), 400
    if query_rounds <= 0:
        return jsonify({"errors": ["rounds must be positive"]}), 400
    return jsonify(run_benchmark(record_count=record_count, query_rounds=query_rounds))


if __name__ == "__main__":
    app.run(debug=True)
