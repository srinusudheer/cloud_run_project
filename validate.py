from pydantic import BaseModel, Field, condecimal, conint, ValidationError
from typing import List
from datetime import datetime

class Item(BaseModel):
    sku: str
    name: str
    qty: conint(gt=0)
    unit_price: condecimal(gt=0)

class ShippingAddress(BaseModel):
    line1: str
    line2: str
    city: str
    state: str
    postal_code: str
    country: str

class Order(BaseModel):
    order_id: str
    customer_id: str
    order_date: datetime
    source: str
    items: List[Item]
    shipping_address: ShippingAddress
    payment_method: str
    total_amount: condecimal(gt=0)


import json
from pyspark.sql.functions import udf
from pyspark.sql.types import BooleanType


def validate_order_udf(order_dict):
    try:
        validated = Order.model_validate(order_dict)
        return validated
    except ValidationError as ve:
        print(f"[ValidationError] Invalid order data: {ve}")
        return None
    except Exception as e:  
        print(f"[Error] Unexpected exception: {e}")
        return None

# the below code for pyspark udf is commented out as it is not used in the current context
# def validate_order(row_json: str) -> bool:
#     try:
#         Order.model_validate(data)
#         return True
#     except ValidationError:
#         return False
#     except Exception:
#         return False


# validate_order_udf = udf(validate_order, BooleanType())