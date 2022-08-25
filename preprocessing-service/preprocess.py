# Standard Library
import asyncio
import json
import logging
import sys
import time

# Third Party
from opni_proto.log_anomaly_payload_pb import Payload, PayloadList
import pandas as pd
from masker import LogMasker
from opni_nats import NatsWrapper

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(message)s")

nw = NatsWrapper()

async def consume_logs(mask_logs_pretrained_queue):
    async def subscribe_handler(msg):
        payload_data = msg.data
        log_payload = Payload()
        await mask_logs_pretrained_queue.put(log_payload.parse(payload_data))


    await nw.subscribe(
        nats_subject="raw_logs",
        nats_queue="workers",
        payload_queue=mask_logs_pretrained_queue,
        subscribe_handler=subscribe_handler,
    )


async def mask_logs(queue, workload_parameters):
    masker = LogMasker()
    last_time = time.time()
    pending_list = []
    while True:
        payload = await queue.get()
        pending_list.append(payload)
        start_time = time.time()
        if start_time - last_time >= 1 or len(pending_list) >= 128:
            payload_data_df = pd.DataFrame(pending_list)
            last_time = start_time
            pending_list = []
            await run(payload_data_df, masker, workload_parameters)

async def run(payload_data_df, masker, workload_parameters):
    logging.info(f"processing {len(payload_data_df)} logs...")
    payload_data_df["log"] = payload_data_df["log"].str.strip()
    filtered_masked_logs = []

    for index, row in payload_data_df.iterrows():
        try:
            if row["log_type"] == "workload":
                for cluster_id in workload_parameters:
                    for namespace_name in workload_parameters[cluster_id]:
                        for pod_name in workload_parameters[cluster_id][namespace_name]:
                            if cluster_id == row["cluster_id"] and namespace_name == row["namespace_name"] and pod_name == row["pod_name"]:
                                row["masked_log"] = masker.mask(row["log"])
                                filtered_masked_logs.append(row)
            else:
                row["masked_log"] = masker.mask(row["log"])
                filtered_masked_logs.append(row)
        except Exception as e:
            row["masked_log"] = row["log"]
            filtered_masked_logs.append(row)
    payload_data_df = pd.DataFrame(filtered_masked_logs)
    if len(payload_data_df) > 0:
        pretrained_model_logs_df = payload_data_df.loc[(payload_data_df["log_type"] != "workload")]
        workload_logs_df = payload_data_df.loc[(payload_data_df["log_type"] == "workload")]

        if len(pretrained_model_logs_df) > 0:
            pretrained_models_list = list(map(lambda row: Payload(*row), pretrained_model_logs_df.values))
            protobuf_pretrained_payload = PayloadList(items=pretrained_models_list)
            await nw.publish(
                "preprocessed_logs_pretrained_model",
                bytes(protobuf_pretrained_payload),
            )

        if len(workload_logs_df) > 0:
            workload_logs_list = list(map(lambda row: Payload(*row), workload_logs_df.values))
            protobuf_workload_payload = PayloadList(items=workload_logs_list)
            await nw.publish(
                "preprocessed_logs_workload",
                bytes(protobuf_workload_payload),
            )

async def init_nats():
    logging.info("Attempting to connect to NATS")
    await nw.connect()


if __name__ == "__main__":
    with open("workload_parameters.json","r") as wp:
        workload_parameters = json.load(wp)
    loop = asyncio.get_event_loop()
    mask_logs_queue = asyncio.Queue(loop=loop)
    nats_consumer_coroutine = consume_logs(mask_logs_queue)
    mask_logs_coroutine = mask_logs(mask_logs_queue, workload_parameters)

    task = loop.create_task(init_nats())
    loop.run_until_complete(task)

    loop.run_until_complete(
        asyncio.gather(nats_consumer_coroutine, mask_logs_coroutine)
    )
    try:
        loop.run_forever()
    finally:
        loop.close()
