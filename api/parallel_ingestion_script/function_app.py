import os
import sys
import json
import logging
import asyncio
import threading
import concurrent.futures

import azure.functions as func
import azure.durable_functions as df

from src.run_ingestion import main

sys.path.insert(0, os.path.dirname(__file__))

app = df.DFApp(http_auth_level=func.AuthLevel.ANONYMOUS)

@app.route(route="adxingestor/start_orchestrator")
@app.durable_client_input(client_name="client")
async def adxingestor(req: func.HttpRequest, client) -> func.HttpResponse:
    logging.info("[INFO] --> HTTP trigger function processed a request.")

    try:
        req_body = req.get_json()
        input_data = req_body if req_body else {}
    except ValueError:
        input_data = {}

    instance_id = await client.start_new("start_orchestrator", client_input=input_data)

    logging.info(f"[INFO] --> Started orchestration with ID = '{instance_id}'.")
    
    return client.create_check_status_response(req, instance_id)

@app.orchestration_trigger(context_name="context")
def start_orchestrator(context: df.DurableOrchestrationContext):
    logging.info("[INFO] --> Started orchestration")
    
    input_data = context.get_input()

    result = yield context.call_activity("start_ingestion", input_data)

    logging.info(f"[INFO] --> Orchestration completed with result: {result}")

    return result

@app.activity_trigger(input_name="payload")
def start_ingestion(payload) -> dict:
    logging.info("[INFO] --> Started ingestion activity")
    
    try:
        try:
            loop = asyncio.get_running_loop()

            logging.info("[INFO] --> Running in existing event loop")
            
            def run_main_in_thread():
                new_loop = asyncio.new_event_loop()
                asyncio.set_event_loop(new_loop)
                try:
                    return new_loop.run_until_complete(main())
                finally:
                    new_loop.close()
            
            with concurrent.futures.ThreadPoolExecutor() as executor:
                executor.submit(run_main_in_thread)

        except RuntimeError:
            logging.info("[INFO] --> No existing event loop, using asyncio.run()")
            result = asyncio.run(main())
        
        return {
            "result": result if result else "Completed"
        }
        
    except Exception as e:
        error_msg = f"Ingestion failed: {str(e)}"
        logging.error(f"[ERROR] --> {error_msg}")
        return {
            "status": "error", 
            "message": error_msg,
            "result": None
        }

@app.route(route="status/{instanceId}")
@app.durable_client_input(client_name="client")
async def get_status(req: func.HttpRequest, client) -> func.HttpResponse:
    instance_id = req.route_params.get('instanceId')
    
    status = await client.get_status(instance_id)
    
    if status is None:
        return func.HttpResponse(
            json.dumps({"error": "Instance not found"}),
            status_code=404,
            mimetype="application/json"
        )
    
    return func.HttpResponse(
        json.dumps({
            "instanceId": status.instance_id,
            "runtimeStatus": status.runtime_status.name,
            "input": status.input_,
            "output": status.output,
            "createdTime": status.created_time.isoformat() if status.created_time else None,
            "lastUpdatedTime": status.last_updated_time.isoformat() if status.last_updated_time else None
        }, default=str),
        mimetype="application/json"
    )

@app.route(route="terminate/{instanceId}")
@app.durable_client_input(client_name="client")
async def terminate_orchestration(req: func.HttpRequest, client) -> func.HttpResponse:
    instance_id = req.route_params.get('instanceId')
    reason = req.params.get('reason', 'Terminated via HTTP request')
    
    await client.terminate(instance_id, reason)
    
    return func.HttpResponse(
        json.dumps({"message": f"Orchestration {instance_id} terminated", "reason": reason}),
        mimetype="application/json"
    )