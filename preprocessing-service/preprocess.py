# Standard Library
import asyncio
import json
import logging
import os
import time

# Third Party
from elasticsearch import AsyncElasticsearch
from opni_proto.log_anomaly_payload_pb import Payload, PayloadList
from masker import LogMasker
from opni_nats import NatsWrapper

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(message)s")
workload_parameters_dict = dict()
nw = NatsWrapper()

ES_ENDPOINT = os.environ["ES_ENDPOINT"]
ES_USERNAME = os.getenv("ES_USERNAME", "admin")
ES_PASSWORD = os.getenv("ES_PASSWORD", "admin")

es_instance = AsyncElasticsearch(
    [ES_ENDPOINT],
    port=9200,
    http_auth=(ES_USERNAME, ES_PASSWORD),
    verify_certs=False,
    use_ssl=True,
)

async def consume_logs(mask_logs_queue):
    async def pretrained_subscribe_handler(msg):
        payload_data = msg.data
        log_payload = Payload()
        await mask_logs_queue.put(log_payload.parse(payload_data))

    async def workload_parameters_handler(msg):
        global workload_parameters_dict
        workload_parameters_dict = json.loads(msg.data.decode())["workloads"]

    await nw.subscribe(
        nats_subject="raw_logs",
        nats_queue="workers",
        payload_queue=mask_logs_queue,
        subscribe_handler=pretrained_subscribe_handler,
    )

    await nw.subscribe(
        nats_subject="model_workload_parameters",
        nats_queue="workers",
        subscribe_handler=workload_parameters_handler,
    )

async def get_latest_workload():
    global workload_parameters_dict
    try:
        res = await nw.get_bucket("model-training-parameters")
        bucket_payload = await res.get("modelTrainingParameters")
        workload_parameters_dict = json.loads(bucket_payload.decode())
    except Exception as e:
        logging.error(e)

def verify_workload(payload):
    if payload.cluster_id in workload_parameters_dict and payload.namespace_name in workload_parameters_dict[payload.cluster_id] and payload.deployment in workload_parameters_dict[payload.cluster_id][payload.namespace_name]:
        return True
    return False


async def mask_logs(queue):
    masker = LogMasker()
    last_time = time.time()
    pending_list = []
    while True:
        payload = await queue.get()
        pending_list.append(payload)
        start_time = time.time()
        if start_time - last_time >= 1 or len(pending_list) >= 128:
            payload_list = PayloadList(items=pending_list)
            last_time = start_time
            pending_list = []
            await run(payload_list, masker)

async def run(payload_list, masker):
    logging.info(f"processing {len(payload_list.items)} logs...")
    start_time = time.time()
    filtered_workload_logs = []
    filtered_pretrained_logs = []
    for payload in payload_list.items:
        try:
            if payload.log_type == "workload":
                if verify_workload(payload):
                    payload.masked_log = masker.mask(payload.log)
                    filtered_workload_logs.append(payload)
            else:
                payload.masked_log = masker.mask(payload.log)
                filtered_pretrained_logs.append(payload)
        except Exception as e:
            continue


    if len(filtered_pretrained_logs) > 0:
        protobuf_pretrained_payload = PayloadList(items=filtered_pretrained_logs)
        await nw.publish(
            "preprocessed_logs_pretrained_model",bytes(protobuf_pretrained_payload),)

    if len(filtered_workload_logs) > 0:
        protobuf_workload_payload = PayloadList(items=filtered_workload_logs)
        await nw.publish("preprocessed_logs_workload",bytes(protobuf_workload_payload),)
    end_time = time.time()
    logging.info("Time taken to process {} logs is {} seconds".format(len(payload_list.items), end_time - start_time))

async def init_nats():
    logging.info("Attempting to connect to NATS")
    await nw.connect()


if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    mask_logs_queue = asyncio.Queue(loop=loop)
    nats_consumer_coroutine = consume_logs(mask_logs_queue)
    mask_logs_coroutine = mask_logs(mask_logs_queue)

    init_nats_task = loop.create_task(init_nats())
    loop.run_until_complete(init_nats_task)
    latest_workload_task = loop.create_task(get_latest_workload())
    loop.run_until_complete(latest_workload_task)

    loop.run_until_complete(
        asyncio.gather(nats_consumer_coroutine, mask_logs_coroutine)
    )
    try:
        loop.run_forever()
    finally:
        loop.close()
