# Standard Library
import asyncio
import json
import logging
import os
import sys
import time

# Third Party
from elasticsearch import AsyncElasticsearch
from opni_proto.log_anomaly_payload_pb import Payload, PayloadList
import pandas as pd
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
        workload_parameters_dict = json.loads(msg.data.decode())
        logging.info(workload_parameters_dict)

    await nw.subscribe(
        nats_subject="raw_logs",
        nats_queue="workers",
        payload_queue=mask_logs_queue,
        subscribe_handler=pretrained_subscribe_handler,
    )

    await nw.subscribe(
        nats_subject="workload_parameters",
        nats_queue="workers",
        subscribe_handler=workload_parameters_handler,
    )

async def get_latest_workload():
    global workload_parameters_dict
    query_body = {"sort": [{"time": {"order": "desc"}}],"query": {"match_all": {}}}
    try:
        latest_workload = await es_instance.search(index="model-training-parameters",body=query_body,size=1)
        workload_parameters_dict = json.loads(latest_workload["hits"]["hits"][0]["_source"]["parameters"])
    except Exception as e:
        logging.error(f"{e}")

def verify_workload(payload):
    logging.info(workload_parameters_dict)
    logging.info(payload.cluster_id)
    logging.info(payload.namespace_name)
    if payload.pod_name == 'paymentservice-78c54474d-7qhff':
        logging.info("here it is")
    if payload.pod_name == "paymentservice-78c54474d-7qhff":
        logging.info("double quotes lies the issue")
    logging.info("pod name is {}".format(payload.pod_name))
    for cluster_id in workload_parameters_dict:
        logging.info(cluster_id)
        if payload.cluster_id != cluster_id:
            continue
        for namespace_name in workload_parameters_dict[cluster_id]:
            logging.info(namespace_name)
            if payload.namespace_name != namespace_name:
                continue
            for pod_name in workload_parameters_dict[cluster_id][namespace_name]:
                logging.info(pod_name)
                if pod_name == payload.pod_name:
                    logging.info("over here. match made")
                    logging.info(payload.pod_name)
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
    filtered_workload_logs = []
    filtered_pretrained_logs = []
    for payload in payload_list.items:
        try:
            if payload.log_type == "workload":
                logging.info("workload log here")
                if verify_workload(payload):
                    logging.info(payload.pod_name)
                    logging.info(workload_parameters_dict)
                    payload.masked_log = masker.mask(payload.log)
                    filtered_workload_logs.append(payload)
            else:
                logging.info(payload.log_type)
                payload.masked_log = masker.mask(payload.log)
                filtered_pretrained_logs.append(payload)
        except Exception as e:
            continue


    if len(filtered_pretrained_logs) > 0:
        logging.info("over here.")
        protobuf_pretrained_payload = PayloadList(items=filtered_pretrained_logs)
        await nw.publish(
            "preprocessed_logs_pretrained_model",bytes(protobuf_pretrained_payload),)

    if len(filtered_workload_logs) > 0:
        protobuf_workload_payload = PayloadList(items=filtered_workload_logs)
        await nw.publish("preprocessed_logs_workload",bytes(protobuf_workload_payload),)

async def init_nats():
    logging.info("Attempting to connect to NATS")
    await nw.connect()


if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    mask_logs_queue = asyncio.Queue(loop=loop)
    workload_parameters_queue = asyncio.Queue(loop=loop)
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
