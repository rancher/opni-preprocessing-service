# Standard Library
import asyncio
import json
import subprocess
import time
from queue import Queue
from subprocess import Popen
from threading import Thread

# Third Party
import pandas as pd
from nats.aio.client import Client as NATS
from opni_nats import NatsWrapper
from pytest import fixture

nw = NatsWrapper()
queue = Queue()


def test_pps_happy_path_rke_kubelet():
    
    # This test is to verify the happy path functionality of the Preprocessing Service (PPS). 

    logdata = {"filename":{"0":"/var/lib/rancher/rke/log/kubelet_88e3417029a5a4223a8b4b0e2d90de63f405caea8ba9e90a7d9add8c3ca9661f.log"},
    "log":{"0":"KLOG Date mask: I1027 15:52:49.618961 Num mask: 12995 Token with Digit mask: 86] IP One mask: \"172.31.5.2\"\n Email mask: \"someone@example.com\"\n UTC Date mask: \"2021-10-22T20:38:46.748885552Z\"\n Path mask: \"/product\/6E92ZMYYFZ\"\n URL mask: \"http://www.google.com\"\n"},
    "stream":{"0":"stderr"},"time":{"0":"2021-10-27T15:52:49.619092Z"},"time_length":{"0":30},"window_dt":{"0":1635349920000},"window_start_time_ns":{"0":1635349920000000000},
    "_id":{"0":"16353499696190920000000000000000000"}}

    # NATs subscribe and publish manager
    subscribe_and_publish(logdata, "preprocessed_logs_control_plane", "raw_logs")
    
    wait_for_seconds(2)

    loop = asyncio.get_event_loop()
    loop.stop()

    if queue.empty():
        raise Exception("Queue is empty")
    else:
        pp_cp_payload = queue.get()
        queue.task_done()
        masked_logs = str(pp_cp_payload["masked_log"])
        assert "num mask : <num>" in masked_logs
        assert "klog date mask : <klog_date>" in masked_logs
        assert "utc date mask : <utc_date>" in masked_logs
        assert "path mask : <path>" in masked_logs
        assert "url mask : <url>" in masked_logs
        assert "email mask : <email_address>" in masked_logs
        assert "ip one mask : <ip>" in masked_logs
        assert "token with digit mask : <token_with_digit>" in masked_logs
        assert "True" in str(pp_cp_payload["is_control_plane_log"])
        assert "kubelet" in str(pp_cp_payload["kubernetes_component"])
        print("PP_cp_payload: ", pp_cp_payload)


def test_pps_happy_path_rke_kube_proxy():
    
    # This test is to verify the happy path functionality of the Preprocessing Service (PPS). 

    logdata = {"filename":{"0":"/var/lib/rancher/rke/log/kube-proxy_97105a1260a6d6c50094817ee4d87a2893dcc2b71b8d5cc06f921439d80784f6.log"},
    "log":{"0":"KLOG Date mask: I1027 15:52:49.618961 Num mask: 12995 Token with Digit mask: 86] IP One mask: \"172.31.5.2\"\n Email mask: \"someone@example.com\"\n UTC Date mask: \"2021-10-22T20:38:46.748885552Z\"\n Path mask: \"/product\/6E92ZMYYFZ\"\n URL mask: \"http://www.google.com\"\n"},
    "stream":{"0":"stderr"},"time":{"0":"2021-10-27T15:52:49.619092Z"},"time_length":{"0":30},"window_dt":{"0":1635349920000},"window_start_time_ns":{"0":1635349920000000000},
    "_id":{"0":"16353499696190920000000000000000000"}}

    # NATs subscribe and publish manager
    subscribe_and_publish(logdata, "preprocessed_logs_control_plane", "raw_logs")
    
    wait_for_seconds(2)

    loop = asyncio.get_event_loop()
    loop.stop()

    if queue.empty():
        raise Exception("Queue is empty")
    else:
        pp_cp_payload = queue.get()
        queue.task_done()
        masked_logs = str(pp_cp_payload["masked_log"])
        assert "num mask : <num>" in masked_logs
        assert "klog date mask : <klog_date>" in masked_logs
        assert "utc date mask : <utc_date>" in masked_logs
        assert "path mask : <path>" in masked_logs
        assert "url mask : <url>" in masked_logs
        assert "email mask : <email_address>" in masked_logs
        assert "ip one mask : <ip>" in masked_logs
        assert "token with digit mask : <token_with_digit>" in masked_logs
        assert "True" in str(pp_cp_payload["is_control_plane_log"])
        assert "kube-proxy" in str(pp_cp_payload["kubernetes_component"])
        print("PP_cp_payload: ", pp_cp_payload)


def test_pps_happy_path_rke_etcd():
    
    # This test is to verify the happy path functionality of the Preprocessing Service (PPS). 

    logdata = {"filename":{"0":"/var/lib/rancher/rke/log/etcd_97105a1260a6d6c50094817ee4d87a2893dcc2b71b8d5cc06f921439d80784f6.log"},
    "log":{"0":"KLOG Date mask: I1027 15:52:49.618961 Num mask: 12995 Token with Digit mask: 86] IP One mask: \"172.31.5.2\"\n Email mask: \"someone@example.com\"\n UTC Date mask: \"2021-10-22T20:38:46.748885552Z\"\n Path mask: \"/product\/6E92ZMYYFZ\"\n URL mask: \"http://www.google.com\"\n"},
    "stream":{"0":"stderr"},"time":{"0":"2021-10-27T15:52:49.619092Z"},"time_length":{"0":30},"window_dt":{"0":1635349920000},"window_start_time_ns":{"0":1635349920000000000},
    "_id":{"0":"16353499696190920000000000000000000"}}

    # NATs subscribe and publish manager
    subscribe_and_publish(logdata, "preprocessed_logs_control_plane", "raw_logs")
    
    wait_for_seconds(2)

    loop = asyncio.get_event_loop()
    loop.stop()

    if queue.empty():
        raise Exception("Queue is empty")
    else:
        pp_cp_payload = queue.get()
        queue.task_done()
        masked_logs = str(pp_cp_payload["masked_log"])
        assert "num mask : <num>" in masked_logs
        assert "klog date mask : <klog_date>" in masked_logs
        assert "utc date mask : <utc_date>" in masked_logs
        assert "path mask : <path>" in masked_logs
        assert "url mask : <url>" in masked_logs
        assert "email mask : <email_address>" in masked_logs
        assert "ip one mask : <ip>" in masked_logs
        assert "token with digit mask : <token_with_digit>" in masked_logs
        assert "True" in str(pp_cp_payload["is_control_plane_log"])
        assert "etcd" in str(pp_cp_payload["kubernetes_component"])
        print("PP_cp_payload: ", pp_cp_payload)


def test_pps_happy_path_rke_kube_apiserver():
    
    # This test is to verify the happy path functionality of the Preprocessing Service (PPS). 

    logdata = {"filename":{"0":"/var/lib/rancher/rke/log/kube-apiserver_97105a1260a6d6c50094817ee4d87a2893dcc2b71b8d5cc06f921439d80784f6.log"},
    "log":{"0":"KLOG Date mask: I1027 15:52:49.618961 Num mask: 12995 Token with Digit mask: 86] IP One mask: \"172.31.5.2\"\n Email mask: \"someone@example.com\"\n UTC Date mask: \"2021-10-22T20:38:46.748885552Z\"\n Path mask: \"/product\/6E92ZMYYFZ\"\n URL mask: \"http://www.google.com\"\n"},
    "stream":{"0":"stderr"},"time":{"0":"2021-10-27T15:52:49.619092Z"},"time_length":{"0":30},"window_dt":{"0":1635349920000},"window_start_time_ns":{"0":1635349920000000000},
    "_id":{"0":"16353499696190920000000000000000000"}}

    # NATs subscribe and publish manager
    subscribe_and_publish(logdata, "preprocessed_logs_control_plane", "raw_logs")
    
    wait_for_seconds(2)

    loop = asyncio.get_event_loop()
    loop.stop()

    if queue.empty():
        raise Exception("Queue is empty")
    else:
        pp_cp_payload = queue.get()
        queue.task_done()
        masked_logs = str(pp_cp_payload["masked_log"])
        assert "num mask : <num>" in masked_logs
        assert "klog date mask : <klog_date>" in masked_logs
        assert "utc date mask : <utc_date>" in masked_logs
        assert "path mask : <path>" in masked_logs
        assert "url mask : <url>" in masked_logs
        assert "email mask : <email_address>" in masked_logs
        assert "ip one mask : <ip>" in masked_logs
        assert "token with digit mask : <token_with_digit>" in masked_logs
        assert "True" in str(pp_cp_payload["is_control_plane_log"])
        assert "kube-apiserver" in str(pp_cp_payload["kubernetes_component"])
        print("PP_cp_payload: ", pp_cp_payload)


def test_pps_happy_path_rke_kube_controller_manager():
    
    # This test is to verify the happy path functionality of the Preprocessing Service (PPS). 

    logdata = {"filename":{"0":"/var/lib/rancher/rke/log/kube-controller-manager_97105a1260a6d6c50094817ee4d87a2893dcc2b71b8d5cc06f921439d80784f6.log"},
    "log":{"0":"KLOG Date mask: I1027 15:52:49.618961 Num mask: 12995 Token with Digit mask: 86] IP One mask: \"172.31.5.2\"\n Email mask: \"someone@example.com\"\n UTC Date mask: \"2021-10-22T20:38:46.748885552Z\"\n Path mask: \"/product\/6E92ZMYYFZ\"\n URL mask: \"http://www.google.com\"\n"},
    "stream":{"0":"stderr"},"time":{"0":"2021-10-27T15:52:49.619092Z"},"time_length":{"0":30},"window_dt":{"0":1635349920000},"window_start_time_ns":{"0":1635349920000000000},
    "_id":{"0":"16353499696190920000000000000000000"}}

    # NATs subscribe and publish manager
    subscribe_and_publish(logdata, "preprocessed_logs_control_plane", "raw_logs")
    
    wait_for_seconds(2)

    loop = asyncio.get_event_loop()
    loop.stop()

    if queue.empty():
        raise Exception("Queue is empty")
    else:
        pp_cp_payload = queue.get()
        queue.task_done()
        masked_logs = str(pp_cp_payload["masked_log"])
        assert "num mask : <num>" in masked_logs
        assert "klog date mask : <klog_date>" in masked_logs
        assert "utc date mask : <utc_date>" in masked_logs
        assert "path mask : <path>" in masked_logs
        assert "url mask : <url>" in masked_logs
        assert "email mask : <email_address>" in masked_logs
        assert "ip one mask : <ip>" in masked_logs
        assert "token with digit mask : <token_with_digit>" in masked_logs
        assert "True" in str(pp_cp_payload["is_control_plane_log"])
        assert "kube-controller-manager" in str(pp_cp_payload["kubernetes_component"])
        print("PP_cp_payload: ", pp_cp_payload)


def test_pps_happy_path_rke_kube_scheduler():
    
    # This test is to verify the happy path functionality of the Preprocessing Service (PPS). 

    logdata = {"filename":{"0":"/var/lib/rancher/rke/log/kube-scheduler_88e3417029a5a4223a8b4b0e2d90de63f405caea8ba9e90a7d9add8c3ca9661f.log"},
    "log":{"0":"KLOG Date mask: I1027 15:52:49.618961 Num mask: 12995 Token with Digit mask: 86] IP One mask: \"172.31.5.2\"\n Email mask: \"someone@example.com\"\n UTC Date mask: \"2021-10-22T20:38:46.748885552Z\"\n Path mask: \"/product\/6E92ZMYYFZ\"\n URL mask: \"http://www.google.com\"\n"},
    "stream":{"0":"stderr"},"time":{"0":"2021-10-27T15:52:49.619092Z"},"time_length":{"0":30},"window_dt":{"0":1635349920000},"window_start_time_ns":{"0":1635349920000000000},
    "_id":{"0":"16353499696190920000000000000000000"}}

    # NATs subscribe and publish manager
    subscribe_and_publish(logdata, "preprocessed_logs_control_plane", "raw_logs")
    
    wait_for_seconds(2)

    loop = asyncio.get_event_loop()
    loop.stop()

    if queue.empty():
        raise Exception("Queue is empty")
    else:
        pp_cp_payload = queue.get()
        queue.task_done()
        masked_logs = str(pp_cp_payload["masked_log"])
        assert "num mask : <num>" in masked_logs
        assert "klog date mask : <klog_date>" in masked_logs
        assert "utc date mask : <utc_date>" in masked_logs
        assert "path mask : <path>" in masked_logs
        assert "url mask : <url>" in masked_logs
        assert "email mask : <email_address>" in masked_logs
        assert "ip one mask : <ip>" in masked_logs
        assert "token with digit mask : <token_with_digit>" in masked_logs
        assert "True" in str(pp_cp_payload["is_control_plane_log"])
        assert "kube-scheduler" in str(pp_cp_payload["kubernetes_component"])
        print("PP_cp_payload: ", pp_cp_payload)


def test_pps_happy_path_rke2_kubelet():
    
    # This test is to verify the happy path functionality of the Preprocessing Service (PPS). 

    logdata = {"filename":{"0":"/var/lib/rancher/rke2/agent/logs/kubelet_88e3417029a5a4223a8b4b0e2d90de63f405caea8ba9e90a7d9add8c3ca9661f.log"},
    "log":{"0":"KLOG Date mask: I1027 15:52:49.618961 Num mask: 12995 Token with Digit mask: 86] IP One mask: \"172.31.5.2\"\n Email mask: \"someone@example.com\"\n UTC Date mask: \"2021-10-22T20:38:46.748885552Z\"\n Path mask: \"/product\/6E92ZMYYFZ\"\n URL mask: \"http://www.google.com\"\n"},
    "stream":{"0":"stderr"},"time":{"0":"2021-10-27T15:52:49.619092Z"},"time_length":{"0":30},"window_dt":{"0":1635349920000},"window_start_time_ns":{"0":1635349920000000000},
    "_id":{"0":"16353499696190920000000000000000000"}}

    # NATs subscribe and publish manager
    subscribe_and_publish(logdata, "preprocessed_logs_control_plane", "raw_logs")
    
    wait_for_seconds(2)

    loop = asyncio.get_event_loop()
    loop.stop()

    if queue.empty():
        raise Exception("Queue is empty")
    else:
        pp_cp_payload = queue.get()
        queue.task_done()
        masked_logs = str(pp_cp_payload["masked_log"])
        assert "num mask : <num>" in masked_logs
        assert "klog date mask : <klog_date>" in masked_logs
        assert "utc date mask : <utc_date>" in masked_logs
        assert "path mask : <path>" in masked_logs
        assert "url mask : <url>" in masked_logs
        assert "email mask : <email_address>" in masked_logs
        assert "ip one mask : <ip>" in masked_logs
        assert "token with digit mask : <token_with_digit>" in masked_logs
        assert "True" in str(pp_cp_payload["is_control_plane_log"])
        assert "kubelet" in str(pp_cp_payload["kubernetes_component"])
        print("PP_cp_payload: ", pp_cp_payload)


def test_pps_happy_path_k3s():
    
    # This test is to verify the happy path functionality of the Preprocessing Service (PPS). 

    logdata = {"filename":{"0":"k3s.log"},
    "log":{"0":"KLOG Date mask: I1027 15:52:49.618961 Num mask: 12995 Token with Digit mask: 86] IP One mask: \"172.31.5.2\"\n Email mask: \"someone@example.com\"\n UTC Date mask: \"2021-10-22T20:38:46.748885552Z\"\n Path mask: \"/product\/6E92ZMYYFZ\"\n URL mask: \"http://www.google.com\"\n"},
    "stream":{"0":"stderr"},"time":{"0":"2021-10-27T15:52:49.619092Z"},"time_length":{"0":30},"window_dt":{"0":1635349920000},"window_start_time_ns":{"0":1635349920000000000},
    "_id":{"0":"16353499696190920000000000000000000"}}

    # NATs subscribe and publish manager
    subscribe_and_publish(logdata, "preprocessed_logs_control_plane", "raw_logs")
    
    wait_for_seconds(2)

    loop = asyncio.get_event_loop()
    loop.stop()

    if queue.empty():
        raise Exception("Queue is empty")
    else:
        pp_cp_payload = queue.get()
        queue.task_done()
        masked_logs = str(pp_cp_payload["masked_log"])
        assert "num mask : <num>" in masked_logs
        assert "klog date mask : <klog_date>" in masked_logs
        assert "utc date mask : <utc_date>" in masked_logs
        assert "path mask : <path>" in masked_logs
        assert "url mask : <url>" in masked_logs
        assert "email mask : <email_address>" in masked_logs
        assert "ip one mask : <ip>" in masked_logs
        assert "token with digit mask : <token_with_digit>" in masked_logs
        assert "True" in str(pp_cp_payload["is_control_plane_log"])
        assert "k3s" in str(pp_cp_payload["kubernetes_component"])
        print("PP_cp_payload: ", pp_cp_payload)


def test_pps_happy_path_k3s():
    
    # This test is to verify the happy path functionality of the Preprocessing Service (PPS). 

    logdata = {"filename":{"0":"k3s.log"},
    "log":{"0":"KLOG Date mask: I1027 15:52:49.618961 Num mask: 12995 Token with Digit mask: 86] IP One mask: \"172.31.5.2\"\n Email mask: \"someone@example.com\"\n UTC Date mask: \"2021-10-22T20:38:46.748885552Z\"\n Path mask: \"/product\/6E92ZMYYFZ\"\n URL mask: \"http://www.google.com\"\n"},
    "stream":{"0":"stderr"},"time":{"0":"2021-10-27T15:52:49.619092Z"},"time_length":{"0":30},"window_dt":{"0":1635349920000},"window_start_time_ns":{"0":1635349920000000000},
    "_id":{"0":"16353499696190920000000000000000000"}}

    # NATs subscribe and publish manager
    subscribe_and_publish(logdata, "preprocessed_logs_control_plane", "raw_logs")
    
    wait_for_seconds(2)

    loop = asyncio.get_event_loop()
    loop.stop()

    if queue.empty():
        raise Exception("Queue is empty")
    else:
        pp_cp_payload = queue.get()
        queue.task_done()
        masked_logs = str(pp_cp_payload["masked_log"])
        assert "num mask : <num>" in masked_logs
        assert "klog date mask : <klog_date>" in masked_logs
        assert "utc date mask : <utc_date>" in masked_logs
        assert "path mask : <path>" in masked_logs
        assert "url mask : <url>" in masked_logs
        assert "email mask : <email_address>" in masked_logs
        assert "ip one mask : <ip>" in masked_logs
        assert "token with digit mask : <token_with_digit>" in masked_logs
        assert "True" in str(pp_cp_payload["is_control_plane_log"])
        assert "k3s" in str(pp_cp_payload["kubernetes_component"])
        print("PP_cp_payload: ", pp_cp_payload)


def test_pps_happy_path_not_control_plane():
    
    # This test is to verify the happy path functionality of the Preprocessing Service (PPS). 

    logdata = {"log":{"0":"KLOG Date mask: I1027 15:52:49.618961 Num mask: 12995 Token with Digit mask: 86] IP One mask: \"172.31.5.2\"\n Email mask: \"someone@example.com\"\n UTC Date mask: \"2021-10-22T20:38:46.748885552Z\"\n Path mask: \"/product\/6E92ZMYYFZ\"\n URL mask: \"http://www.google.com\"\n"},
    "stream":{"0":"stderr"},"time":{"0":"2021-10-27T15:52:49.619092Z"},"time_length":{"0":30},"window_dt":{"0":1635349920000},"window_start_time_ns":{"0":1635349920000000000},
    "_id":{"0":"16353499696190920000000000000000000"}}

    # NATs subscribe and publish manager
    subscribe_and_publish(logdata, "preprocessed_logs", "raw_logs")
    
    wait_for_seconds(2)

    loop = asyncio.get_event_loop()
    loop.stop()

    if queue.empty():
        raise Exception("Queue is empty")
    else:
        pp_cp_payload = queue.get()
        queue.task_done()
        masked_logs = str(pp_cp_payload["masked_log"])
        assert "num mask : <num>" in masked_logs
        assert "klog date mask : <klog_date>" in masked_logs
        assert "utc date mask : <utc_date>" in masked_logs
        assert "path mask : <path>" in masked_logs
        assert "url mask : <url>" in masked_logs
        assert "email mask : <email_address>" in masked_logs
        assert "ip one mask : <ip>" in masked_logs
        assert "token with digit mask : <token_with_digit>" in masked_logs
        assert "False" in str(pp_cp_payload["is_control_plane_log"])
        assert "'0': ''" in str(pp_cp_payload["kubernetes_component"])
        print("PP_cp_payload: ", pp_cp_payload)


def wait_for_seconds(seconds):
    start_time = time.time()
    while time.time() - start_time < seconds:
        continue


async def consume_logs(subject):

    async def subscribe_handler(msg):
        sbj = msg.subject
        print('Subscribing to the "' + subject + '" subject')
        payload_data = msg.data.decode()

        if sbj == "raw_logs" or sbj == "preprocessed_logs_control_plane" or sbj == "preprocessed_logs":
            payload = json.loads(payload_data)
            queue.put(payload)
        else:
            raise Exception("Subscribe subject is unknown. Verify Subscribe subject.")

    await nw.subscribe(
        nats_subject=subject, 
        subscribe_handler=subscribe_handler,
    )


async def init_nats():
    print("Attempting to connect to NATS")
    await nw.connect()
    assert nw.connect().__init__


def start_background_loop(loop: asyncio.AbstractEventLoop) -> None:
    asyncio.set_event_loop(loop)
    loop.run_forever()


def subscribe_and_publish(logdata, sub_subject, pub_subject):

    loop = asyncio.get_event_loop()

    nats_consumer_coroutine = consume_logs(sub_subject)
    nats_publisher_coroutine = publish_logs(logdata, pub_subject)

    t = Thread(target=start_background_loop, args=(loop,), daemon=True)
    t.start()

    asyncio.run_coroutine_threadsafe(init_nats(), loop)
    asyncio.run_coroutine_threadsafe(nats_consumer_coroutine, loop)
    asyncio.run_coroutine_threadsafe(nats_publisher_coroutine, loop)

    return t


async def publish_logs(logdata, subject):
    print('Publishing to the "' + subject + '" subject')
    print("LOGDATA: ", pd.DataFrame(logdata).to_json().encode())
    await nw.publish(
        nats_subject=subject,
        payload_df=pd.DataFrame(logdata).to_json().encode(),)


def start_process(command):
    try:
        return subprocess.run(command, shell=True)
    except subprocess.CalledProcessError as e:
        return None
