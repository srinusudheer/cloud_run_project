# transform.py (pure Python version)
from datetime import datetime
import uuid


def transform_data(order: dict) -> list[dict]:
    """
    Transforms an order JSON into a list of flattened item records.
    Each item in the order becomes its own row.
    """
    transformed_rows = []

    # Extract shared fields
    base_fields = {
        "order_id": order["order_id"],
        "customer_id": order["customer_id"],
        "order_date": order["order_date"],
        "source": order["source"],
        "payment_method": order["payment_method"],
        "total_amount": float(order["total_amount"]),
        "shipping_line1": order["shipping_address"]["line1"],
        "shipping_line2": order["shipping_address"]["line2"],
        "shipping_city": order["shipping_address"]["city"],
        "shipping_state": order["shipping_address"]["state"],
        "shipping_postal_code": order["shipping_address"]["postal_code"],
        "shipping_country": order["shipping_address"]["country"],
        "processing_id": uuid.uuid4().hex,
        "processed_at": datetime.utcnow().isoformat() + "Z"
    }

    # Flatten items array
    for item in order["items"]:
        row = {
            **base_fields,
            "item_sku": item["sku"],
            "item_name": item["name"],
            "item_qty": item["qty"],
            "item_unit_price": float(item["unit_price"])

        }
        transformed_rows.append(row)
    

    return transformed_rows

# order = {
#   "order_id":      "ORD-20250426-1001",
#   "customer_id":   "CUST-100234",
#   "order_date":    "2025-04-26T22:30:45Z",
#   "source":        "web_portal",
#   "items": [
#     {
#       "sku":         "WM-12345",
#       "name":        "Wireless Ergonomic Mouse",
#       "qty":         2,
#       "unit_price":  25.99
#     },
#     {
#       "sku":         "KB-67890",
#       "name":        "Mechanical Gaming Keyboard",
#       "qty":         1,
#       "unit_price":  79.99
#     }
#   ],
#   "shipping_address": {
#     "line1":       "123 Tech Park Rd",
#     "line2":       "Suite 400",
#     "city":        "San Francisco",
#     "state":       "CA",
#     "postal_code": "94107",
#     "country":     "USA"
#   },
#   "payment_method": "credit_card",
#   "total_amount":   131.97
# }
# print(transform_data(order))