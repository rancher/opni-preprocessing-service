# Standard Library
import asyncio
import json
import logging
import time

# Third Party
from google.protobuf import json_format
import pandas as pd
from masker import LogMasker
from opni_nats import NatsWrapper
import payload_pb2

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(message)s")

nw = NatsWrapper()

async def consume_logs(mask_logs_queue):
    async def subscribe_handler(msg):
        payload_data = msg.data
        log_payload = payload_pb2.Payload()
        await mask_logs_queue.put(log_payload.FromString(payload_data))


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
        payload = await queue.get()
        pending_list.append(json_format.MessageToDict(payload))
        start_time = time.time()
        if start_time - last_time >= 1 or len(pending_list) >= 128:
            payload_data_df = pd.DataFrame(pending_list)
            last_time = start_time
            pending_list = []
            await run(payload_data_df, masker)

async def run(payload_data_df, masker):
    logging.info(f"processing {len(payload_data_df)} logs...")
    payload_data_df["log"] = payload_data_df["log"].str.strip()
    masked_logs = []

    for index, row in payload_data_df.iterrows():
        try:
            masked_logs.append(masker.mask(row["log"]))
        except Exception as e:
            masked_logs.append(row["log"])
    payload_data_df["maskedLog"] = masked_logs

    pretrained_model_logs_dict = (payload_data_df.loc[(payload_data_df["logType"] != "workload")]).to_dict('records')

    if len(pretrained_model_logs_dict) > 0:
        protobuf_dict = {"items": pretrained_model_logs_dict}
        protobuf_payload = payload_pb2.PayloadList()
        serialized_logs = (json_format.ParseDict(protobuf_dict, protobuf_payload)).SerializeToString()
        await nw.publish(
            "preprocessed_logs_pretrained_model",
            serialized_logs,
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
