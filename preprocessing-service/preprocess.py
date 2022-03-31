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
    df["_op_type"] = "update"
    df["_index"] = "logs"
    doc_keywords = set(["_op_type", "_index", "_id", "doc"])
    for index, document in df.iterrows():
        doc_dict = document[pd.notnull(document)].to_dict()
        doc_dict["doc"] = {}
        doc_dict_keys = list(doc_dict.keys())
        for k in doc_dict_keys:
            if not k in doc_keywords:
                doc_dict["doc"][k] = doc_dict[k]
                del doc_dict[k]
        #Comment out the anomaly_predicted_count and nulog_anomaly values as those will both be used for workload logs.
        #doc_dict["doc"]["anomaly_predicted_count"] = 0
        #doc_dict["doc"]["nulog_anomaly"] = False
        doc_dict["doc"]["drain_control_plane_template_matched"] = ""
        doc_dict["doc"]["anomaly_level"] = "Normal"
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
        if (
            "log" not in payload_data_df.columns
            and "MESSAGE" in payload_data_df.columns
        ):
            payload_data_df["log"] = payload_data_df["MESSAGE"]
        if (
            "message" in payload_data_df.columns
            and "MESSAGE" in payload_data_df.columns
        ):
            payload_data_df.loc[
                payload_data_df["log"] == "", ["log"]
            ] = payload_data_df["MESSAGE"]
        if (
            "log" not in payload_data_df.columns
            and "message" not in payload_data_df.columns
        ):
            continue
        payload_data_df["log"] = payload_data_df["log"].str.strip()
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
        if "agent" in payload_data_df.columns:
            payload_data_df.loc[
                payload_data_df["agent"] != "support",
                ["is_control_plane_log", "kubernetes_component"],
            ] = [False, ""]
        else:
            payload_data_df["is_control_plane_log"] = False
            payload_data_df["kubernetes_component"] = ""
        # rke1
        if "filename" in payload_data_df.columns:
            rke_filename_filter = payload_data_df["filename"].notnull() & payload_data_df["filename"].str.contains("rke/log/etcd|rke/log/kubelet|/rke/log/kube-apiserver|rke/log/kube-controller-manager|rke/log/kube-proxy|rke/log/kube-scheduler")
            payload_data_df.loc[rke_filename_filter, ["is_control_plane_log"]] = True
            payload_data_df.loc[rke_filename_filter, ["kubernetes_component"]] = payload_data_df[rke_filename_filter]["filename"].apply(lambda x: os.path.basename(x))
            payload_data_df.loc[rke_filename_filter, ["kubernetes_component"]] = payload_data_df[rke_filename_filter]["kubernetes_component"].str.split("_").str[0]

            # k3s openrc
            payload_data_df.loc[
                payload_data_df["filename"].notnull() & payload_data_df["filename"].str.contains(r"k3s\.log"),
                ["is_control_plane_log", "kubernetes_component"],
            ] = [True, "k3s"]
            # rke2 kubelet
            payload_data_df.loc[
                payload_data_df["filename"].notnull() & payload_data_df["filename"].str.contains("rke2/agent/logs/kubelet"),
                ["is_control_plane_log", "kubernetes_component"],
            ] = [True, "kubelet"]
        # k3s/rke2 systemd
        if "COMM" in payload_data_df.columns:
            comm_df_filter = payload_data_df["COMM"].notnull() & payload_data_df["COMM"].str.contains("(k3s|rke2)-(agent|server)|kubelet")
            payload_data_df.loc[comm_df_filter, ["is_control_plane_log"]] = True
            payload_data_df.loc[comm_df_filter, ["kubernetes_component"]] = payload_data_df[comm_df_filter]["COMM"].str.split("-").str[0]
        # rke2/kops/kubeadm static pods
        if "kubernetes.labels.tier" in payload_data_df.columns:
            kube_labels_tier_filter = payload_data_df["kubernetes.labels.tier"].notnull() & payload_data_df["kubernetes.labels.tier"] == "control-plane"
            payload_data_df.loc[kube_labels_tier_filter, ["is_control_plane_log"]] = True
            payload_data_df.loc[kube_labels_tier_filter, ["kubernetes_component"]] = payload_data_df[kube_labels_tier_filter]["kubernetes.labels.component"]

        masked_logs = []
        for index, row in payload_data_df.iterrows():
            masked_logs.append(masker.mask(row["log"], row["is_control_plane_log"]))
        payload_data_df["masked_log"] = masked_logs
        try:
            async for ok, result in async_streaming_bulk(
                es, doc_generator(payload_data_df)
            ):
                action, result = result.popitem()
        except (BulkIndexError, ConnectionTimeout) as exception:
            logging.error(exception)
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
