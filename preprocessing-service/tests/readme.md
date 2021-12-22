## Validation scripts for Preprocessing Service
Validation tests are run in a similar way to integration tests.

## Test Environment Setup:

Setup a Kubernetes cluster and save the Kubeconfig to the `.kube/config` file.

## Install Opni and Run Sonobuoy Tests:

Run the command: 
```
bash ./opni-preprocessing-service/preprocessing-service/tests/sonobuoy/sonobuoy_run.sh
```

Unzip the test results tar.gz file to reveiw the test results.

## To run the Sonobuoy tests after Opni is already installed:

Ensure that the `authMethod` value for the Opni cluster is changed from `nkey` to `username`.
Ensure that the `cluster-nats-client` secret's password value is set to `nats-password`

Run the command: 
```
sonobuoy run \
--kubeconfig ~/.kube/config \
--namespace "opni-sono" \
--plugin https://raw.githubusercontent.com/rancher/opni-preprocessing-service/c628170a2f67442d7661866252c90eff78b20806/preprocessing-service/tests/sonobuoy/opnisono-plugin.yaml
```

Periodically run the following command until the tests are complete:
```
sonobuoy status -n opni-sono
```

Run the following command and a tar file with the test results will be generated in the current directory:
```
sonobuoy retrieve -n opni-sono
```

Unzip the test results tar.gz file to reveiw the test results.

## Helpful docs:
Opni Basic Installation Docs: https://opni.io/deployment/basic/

Opni NATs Configuration Docs: https://opni.io/configuration/nats/

Opni NATs Wrapper: https://github.com/rancher/opni-nats-wrapper

PyTest Docs: https://docs.pytest.org/en/6.2.x/