# Validation scripts for Payload Receiver Service
Validation tests are run in a similar way to integration tests.

## Test Environment Setup:

Setup a Kubernetes cluster and save the kubeconfig into the kube.yaml file

Follow the Basic Installation Instructions in the Opni Docs: https://opni.io/deployment/basic/

Run command: kubectl delete opnicluster -n opni-cluster opni-cluster

Copy and save a local copy of the opnicluster yaml file: https://raw.githubusercontent.com/rancher/opni/main/deploy/manifests/20_cluster.yaml

In the yaml file, modify the authMethod value to be "username"

Run the command: kubectl apply -f {File path for the local yaml file}

## Locate the NATs password

In Rancher's Cluster Explorer view for the Kubernetes cluster, navigate to the Secrets page

Select opni-cluster-nats-client, and get the password from that page

## Install Testing Requirements

Run the command: pip3 install -r /tests/integration/requirements.txt

## Export any needed environment vars:

export NATS_SERVER_URL=nats://localhost:4222
export NATS_USERNAME=nats-user
export NATS_PASSWORD={Password obtained above}

### Now run the tests from the /preprocessing-service/tests/integration dir:

Run the command: pytest -v -s smoke_tests.py

### Helpful docs:
Opni Basic Installation Docs: https://opni.io/deployment/basic/

Opni NATs Configuration Docs: https://opni.io/configuration/nats/

Faker Package Info: https://github.com/joke2k/faker

Opni NATs Wrapper: https://github.com/rancher/opni-nats-wrapper

PyTest Docs: https://docs.pytest.org/en/6.2.x/