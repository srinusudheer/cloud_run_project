import logging
import os
import functions_framework
from flask import jsonify, make_response, request

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
        def write_to_bigquery(enriched):
            table_id = os.environ.get("BQ_TABLE")  # Should be "project.dataset.table"
            if not table_id:
                raise ValueError("BQ_TABLE environment variable not set")

            client = bigquery.Client()
            errors = client.insert_rows_json(table_id, enriched)
            if errors:
                raise RuntimeError(f"Failed to insert rows: {errors}")
            logger.info("Data written to BigQuery successfully")
        write_to_bigquery(enriched)
    except Exception as e:
        logger.error("Failed to write data to BigQuery: %s", e)
        return make_response(jsonify({"error": "Failed to write data to BigQuery"}), 500)
    

    return make_response(jsonify({"message": "Order processed successfully"}), 200) 
   

# For local testing with functions-framework
if __name__ == "__main__":
    from werkzeug.serving import run_simple
    from functions_framework import create_app
    app = create_app("order_event")
    run_simple("0.0.0.0", 8080, app) 
    