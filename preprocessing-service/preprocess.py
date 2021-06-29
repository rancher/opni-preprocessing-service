# Standard Library
import asyncio
import logging
import os

# Third Party
import numpy as np
import pandas as pd
from elasticsearch import AsyncElasticsearch
from elasticsearch.exceptions import ConnectionTimeout
from elasticsearch.helpers import BulkIndexError, async_streaming_bulk
from masker import LogMasker
from opni_nats import NatsWrapper

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(message)s")

ES_ENDPOINT = os.environ["ES_ENDPOINT"]
ES_USERNAME = os.environ["ES_USERNAME"]
ES_PASSWORD = os.environ["ES_PASSWORD"]

es = AsyncElasticsearch(
    [ES_ENDPOINT],
    port=9200,
    http_auth=(ES_USERNAME, ES_PASSWORD),
    http_compress=True,
    verify_certs=False,
    use_ssl=True,
    timeout=10,
    max_retries=5,
    retry_on_timeout=True,
)

nw = NatsWrapper()


async def doc_generator(df):
    df["_index"] = "logs"
    df["anomaly_predicted_count"] = 0
    df["nulog_anomaly"] = False
    df["drain_anomaly"] = False
    df["nulog_confidence"] = -1.0
    df["drain_matched_template_id"] = -1.0
    df["drain_matched_template_support"] = -1.0
    df["anomaly_level"] = "Normal"
    for index, document in df.iterrows():
        doc_dict = document.to_dict()
        yield doc_dict


async def consume_logs(mask_logs_queue):
    async def subscribe_handler(msg):
        payload_data = msg.data.decode()
        await mask_logs_queue.put(pd.read_json(payload_data, dtype={"_id": object}))

    await nw.subscribe(
        nats_subject="raw_logs",
        nats_queue="workers",
        payload_queue=mask_logs_queue,
        subscribe_handler=subscribe_handler,
    )


async def mask_logs(queue):
    masker = LogMasker()
    while True:
        payload_data_df = await queue.get()
        if (
            "log" not in payload_data_df.columns
            and "message" in payload_data_df.columns
        ):
            payload_data_df["log"] = payload_data_df["message"]
        # TODO Retrain controlplane model to support k3s
        elif (
           "log" not in payload_data_df.columns
           and "MESSAGE" in payload_data_df.columns
        ):
           payload_data_df["log"] = payload_data_df["MESSAGE"]
        if (
            "log" not in payload_data_df.columns
            and "message" not in payload_data_df.columns
        ):
            logging.warning("ERROR: No log or message field")
            continue
        payload_data_df["log"] = payload_data_df["log"].str.strip()
        masked_logs = []
        for index, row in payload_data_df.iterrows():
            masked_logs.append(masker.mask(row["log"]))
        payload_data_df["masked_log"] = masked_logs
        # impute NaT with time column if available else use current time
        payload_data_df["time_operation"] = pd.to_datetime(
            payload_data_df["time"], errors="coerce", utc=True
        )
        payload_data_df["timestamp"] = (
            payload_data_df["time_operation"].astype(np.int64) // 10 ** 6
        )
        payload_data_df.drop(columns=["time_operation", "time"], inplace=True)

        # drop redundant field in control plane logs
        payload_data_df.drop(["t.$date"], axis=1, errors="ignore", inplace=True)
        payload_data_df["is_control_plane_log"] = False
        payload_data_df["kubernetes_component"] = ""
        # rke1
        if "filename" in payload_data_df.columns:
            # rke
            payload_data_df["is_control_plane_log"] = payload_data_df[
                "filename"
            ].str.contains(
                "rke/log/etcd|rke/log/kubelet|/rke/log/kube-apiserver|rke/log/kube-controller-manager|rke/log/kube-proxy|rke/log/kube-scheduler"
            )
            payload_data_df["kubernetes_component"] = payload_data_df["filename"].apply(
                lambda x: os.path.basename(x)
            )
            payload_data_df["kubernetes_component"] = (
                payload_data_df["kubernetes_component"].str.split("_").str[0]
            )
            # k3s openrc
            payload_data_df.loc[
                payload_data_df["filename"].str.contains("k3s\.log"),
                ["is_control_plane_log", "kubernetes_component"]
            ] = [True, "k3s"]
        # k3s systemd
        elif "COMM" in payload_data_df.columns:
            payload_data_df["is_control_plane_log"] = ( 
                payload_data_df["COMM"] == "k3s-server" | payload_data_df["COMM"] == "k3s-agent"
            )
            payload_data_df["kubernetes_component"] = (
                payload_data_df["COMM"].str.split("-").str[0]
            )
        # rke2
        elif "kubernetes.labels.tier" in payload_data_df.columns:
            payload_data_df["is_control_plane_log"] = (
                payload_data_df["kubernetes.labels.tier"] == "control-plane"
            )
            payload_data_df["kubernetes_component"] = payload_data_df[
                "kubernetes.labels.component"
            ]
        is_control_log = payload_data_df["is_control_plane_log"] == True
        control_plane_logs_df = payload_data_df[is_control_log]
        app_logs_df = payload_data_df[~is_control_log]

        if len(app_logs_df) > 0:
            await nw.publish("preprocessed_logs", app_logs_df.to_json().encode())

        if len(control_plane_logs_df) > 0:
            await nw.publish(
                "preprocessed_logs_control_plane",
                control_plane_logs_df.to_json().encode(),
            )

        try:
            async for ok, result in async_streaming_bulk(
                es, doc_generator(payload_data_df)
            ):
                action, result = result.popitem()
                if not ok:
                    logging.error("failed to {} document {}".format())
        except (BulkIndexError, ConnectionTimeout) as exception:
            logging.error("Failed to index data")
            logging.error(exception)


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
