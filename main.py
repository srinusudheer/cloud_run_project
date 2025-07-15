import logging
import os
import functions_framework
from flask import jsonify, make_response
from google.api_core.exceptions import NotFound

from validate import validate_order_udf
from transform import transform_data

from google.cloud import bigquery
import os
# test lines

# ——— Logger setup ———
logger = logging.getLogger('order_service')
logger.setLevel(logging.INFO)

if not logger.handlers: # Prevents duplicate handlers if the logger is configured multiple times.

    h = logging.StreamHandler() # Creates a handler that writes logs to the console (stdout).
    h.setFormatter(logging.Formatter(
        "%(asctime)s %(levelname)s %(name)s: %(message)s"
    ))
    logger.addHandler(h)


@functions_framework.http
def order_event(request):
    """
    HTTP Cloud Run Function to handle 'order.created' events.
    Expects a rich JSON payload (see utils/order_utils.validate_payload)
    We are making changes to code in the comment section.
    Hoping for the success now.
    """

    logger.info("Received %s %s", request.method, request.path)
    if request.method != "POST":
        logger.warning("Only POST allowed")
        return make_response(jsonify({"error": "Use POST"}), 405)

    data = request.get_json(silent=True)
    if not data:
        logger.error("No JSON body")
        return make_response(jsonify({"error": "Invalid JSON"}), 400)
    try:
        validate_order_udf(data)
        logger.info("Validation successful")

    except ValueError as e:
        logger.error("Validation failed: %s", e)
        return make_response(jsonify({"error": str(e)}), 400)
    
    enriched = transform_data(data)
    logger.info("Transformation successful")

    try:
        # def write_to_bigquery(enriched):
        #     table_id = os.environ.get("BQ_TABLE")  # Should be "project.dataset.table"
        #     if not table_id:
        #         raise ValueError("BQ_TABLE environment variable not set")

        #     client = bigquery.Client()
        #     errors = client.insert_rows_json(table_id, enriched)
        #     if errors:
        #         raise RuntimeError(f"Failed to insert rows: {errors}")
        #     logger.info("Data written to BigQuery successfully")
        #write_to_bigquery(enriched)
        
        def ensure_table_exists(client, table_id, schema):
            try:
                client.get_table(table_id)  # Check if table exists
                print(f"✅ Table {table_id} exists.")
            except NotFound:
                print(f"⚠️ Table {table_id} not found. Creating it...")
                table = bigquery.Table(table_id, schema=schema)
                client.create_table(table)
                print(f"✅ Table {table_id} created.")
        client = bigquery.Client()
        table_id = os.environ.get("BQ_TABLE")

        schema = [
        bigquery.SchemaField("order_id", "STRING"),
        bigquery.SchemaField("customer_id", "STRING"),
        bigquery.SchemaField("order_date", "TIMESTAMP"),
        bigquery.SchemaField("source", "STRING"),
        bigquery.SchemaField("item_sku", "STRING"),
        bigquery.SchemaField("item_name", "STRING"),
        bigquery.SchemaField("item_qty", "INTEGER"),
        bigquery.SchemaField("item_unit_price", "FLOAT"),
        bigquery.SchemaField("shipping_line1", "STRING"),
        bigquery.SchemaField("shipping_line2", "STRING"),
        bigquery.SchemaField("shipping_city", "STRING"),
        bigquery.SchemaField("shipping_state", "STRING"),
        bigquery.SchemaField("shipping_postal_code", "STRING"),
        bigquery.SchemaField("shipping_country", "STRING"),
        bigquery.SchemaField("payment_method", "STRING"),
        bigquery.SchemaField("total_amount", "FLOAT"),
        bigquery.SchemaField("processing_id", "STRING"),
        bigquery.SchemaField("processed_at", "TIMESTAMP")
        ]

        ensure_table_exists(client, table_id, schema)


    except Exception as e:
        logger.error("Failed to write data to BigQuery: %s", e)
        return make_response(jsonify({"error": "Failed to write data to BigQuery"}), 500)
    

    resp = {
        "status":           "processed",
        "order_id":         enriched["order_id"],
        "processing_id":    enriched["processing_id"],
        "processed_at":     enriched["processed_at"],
        "items_count":      len(enriched["items"]),
        "total_amount":     enriched["total_amount"],
        "payment_method":   enriched["payment_method"],
        "shipping_address": enriched["shipping_address"],
        "message":          "Order received and stored."

    }
    logger.info("Order %s processed successfully after git changes made to original code", enriched["order_id"])
    return make_response(jsonify(resp), 200)
    #return make_response(jsonify({"message": "Order processed successfully"}), 200) 
   

