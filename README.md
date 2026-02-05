# Sales Analytics API

Flask REST API for sales transactions with optimized analytics.

## Run

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
python app.py
```

## Endpoints

### Transactions (CRUD)
- `POST /transactions`
- `GET /transactions`
- `GET /transactions/<id>`
- `PUT /transactions/<id>`
- `DELETE /transactions/<id>`

### Analytics
- `GET /analytics/total-sales-per-product`
- `GET /analytics/top-customers?limit=10`
- `GET /analytics/benchmark?records=50000&rounds=5`

### Filtering (query params)
`GET /transactions?product_id=...&customer_id=...&start_date=...&end_date=...&min_total=...&max_total=...&limit=...&offset=...`

Dates must be ISO 8601 (e.g., `2026-02-05T12:00:00+00:00`).

## Sample Payload

```json
{
  "product_id": "P100",
  "product_name": "Premium Widget",
  "customer_id": "C42",
  "customer_name": "Alex",
  "quantity": 3,
  "price": 49.99,
  "timestamp": "2026-02-05T12:00:00+00:00"
}
```
