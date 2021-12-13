# Validation scripts for Preprocessing Service
Validation tests are run in a similar way to integration tests.

## Test Environment Setup:

Setup a Kubernetes cluster

Follow the first 3 steps pf the Basic Installation Instructions in the Opni Docs: https://opni.io/deployment/basic/

Save a local copy of the Opni yaml file: https://raw.githubusercontent.com/rancher/opni/main/deploy/manifests/20_cluster.yaml

In the yaml file, modify the *authMethod* value to be "username"

Run the command: 
```
kubectl apply -f {File path for the local yaml file}
```

## Locate the NATs password

In Rancher's Cluster Explorer view for the Kubernetes cluster, navigate to the Secrets page

Select opni-nats-client, and get the password from that page

## Download and update Docker Image:

Save a local copy of the Docker Image: 
```
docker pull jamesonmcg/opni-preprocessing-service-sonobuoy:v0.1.0
docker tag jamesonmcg/opni-preprocessing-service-sonobuoy:v0.1.0 {Docker Hub Account}/opni-preprocessing-service-sonobuoy:v0.1.0
```

*Please do not push changes to the originator Docker Hub*

## Set NATs Password Environment Variable:

Open the file /opni-preprocessing-service/preprocessing-service/tests/sonobuoy/Dockerfile.sonobuoy

Set the NATS_PASSWORD= value to the password obtained above, and save the file

Open the file /opni-preprocessing-service/preprocessing-service/tests/sonobuoy/opnisono-plugin.yaml

Set the *image* value to the Docker Hub account and tag: {Docker Hub Account}/opni-preprocessing-service-sonobuoy:v0.1.0

Run the command:
```
docker push {Docker Hub Account}/opni-preprocessing-service-sonobuoy:v0.1.0
```

### Now run the Sonobuoy tests from the repo source dir and get the results:

Run the command: 
```
sonobuoy run \
--kubeconfig {Local kubeconfig.yml file path} \
--namespace "opni-sono" \
--plugin https://raw.githubusercontent.com/rancher/opni-preprocessing-service/a9c6a90eab01328357c7437234d7da021cf36853/preprocessing-service/tests/sonobuoy/opnisono-plugin.yaml
```

Periodically run the command until the tests are complete:
```
sonobuoy status -n opni-sono
```

Run the following command and a tar file with the test results will be generated in the current directory:
```
sonobuoy retrieve -n opni-sono
```

### Helpful docs:
Opni Basic Installation Docs: https://opni.io/deployment/basic/

Opni NATs Configuration Docs: https://opni.io/configuration/nats/

Opni NATs Wrapper: https://github.com/rancher/opni-nats-wrapper

PyTest Docs: https://docs.pytest.org/en/6.2.x/