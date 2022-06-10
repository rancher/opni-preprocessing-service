# Standard Library
import asyncio
import json
import logging
import os
import time

# Third Party
import pandas as pd
from masker import LogMasker
from opni_nats import NatsWrapper

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(message)s")

nw = NatsWrapper()

async def consume_logs(mask_logs_queue):
    async def subscribe_handler(msg):

        payload_data = msg.data.decode()
        await mask_logs_queue.put(json.loads(payload_data))

    await nw.subscribe(
        nats_subject="raw_logs",
        nats_queue="workers",
        payload_queue=mask_logs_queue,
        subscribe_handler=subscribe_handler,
    )

async def mask_logs(queue):
    masker = LogMasker()
    last_time = time.time()
    pending_list = []
    while True:
        json_payload = await queue.get()
        if type(json_payload["log"]) == str:
            pending_list.append(json_payload)
        else:
            payload_data_df = pd.DataFrame(json_payload)
            await run(payload_data_df, masker)

        start_time = time.time()
        if start_time - last_time >= 1 or len(pending_list) >= 128:
            payload_data_df = pd.DataFrame(pending_list)
            last_time = start_time
            pending_list = []
            await run(payload_data_df, masker)

async def run(payload_data_df, masker):
    logging.info(f"processing {len(payload_data_df)} logs...")
    payload_data_df["log"] = payload_data_df["log"].str.strip()
    # drop redundant field in control plane logs
    payload_data_df.drop(["t.$date"], axis=1, errors="ignore", inplace=True)

    masked_logs = []

    for index, row in payload_data_df.iterrows():
        try:
            masked_logs.append(masker.mask(row["log"]))
        except Exception as e:
            masked_logs.append(row["log"])
    payload_data_df["masked_log"] = masked_logs
    payload_data_df["ingest_at"] = payload_data_df["ingest_at"].astype(str)
    payload_data_df["entry_processed"] = False
    payload_data_df["inference_model"] = ""
    payload_data_df.drop(["_type"], axis=1, errors="ignore", inplace=True)
    payload_data_df.drop(["_version"], axis=1, errors="ignore", inplace=True)

    pretrained_model_logs_df = payload_data_df.loc[
        (payload_data_df["log_type"] != "workload")
    ]

    if len(pretrained_model_logs_df) > 0:
        await nw.publish(
            "preprocessed_logs_pretrained_model",
            pretrained_model_logs_df.to_json().encode(),
        )



async def init_nats():
    logging.info("Attempting to connect to NATS")
    await nw.connect()


if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    mask_logs_queue = asyncio.Queue(loop=loop)
    nats_consumer_coroutine = consume_logs(mask_logs_queue)
    mask_logs_coroutine = mask_logs(mask_logs_queue)

    task = loop.create_task(init_nats())
    loop.run_until_complete(task)

    loop.run_until_complete(
        asyncio.gather(nats_consumer_coroutine, mask_logs_coroutine)
    )
    try:
        loop.run_forever()
    finally:
        loop.close()
