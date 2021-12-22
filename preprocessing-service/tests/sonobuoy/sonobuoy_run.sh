#!/bin/bash
# sonobuoy_run.sh
# assume kubeconfig is already set
kubectl apply -f https://github.com/jetstack/cert-manager/releases/download/v1.5.4/cert-manager.yaml
sleep 5
kubectl  apply -f https://raw.githubusercontent.com/rancher/opni/main/deploy/manifests/00_crds.yaml
sleep 2
kubectl  apply -f https://raw.githubusercontent.com/rancher/opni/main/deploy/manifests/01_rbac.yaml
sleep 2
kubectl  apply -f https://raw.githubusercontent.com/rancher/opni/main/deploy/manifests/10_operator.yaml
sleep 4
curl https://raw.githubusercontent.com/rancher/opni/main/deploy/manifests/20_cluster.yaml > 20_cluster.yaml
sed -i 's/nkey/username/' 20_cluster.yaml
sed -i -e '$a\    passwordFrom:\' 20_cluster.yaml
sed -i -e '$a\      name: cluster-nats-client\' 20_cluster.yaml
sed -i -e '$a\      key: password\' 20_cluster.yaml
sed -i 's/prometheuus/prometheus/' 20_cluster.yaml
kubectl create namespace opni
kubectl create secret generic cluster-nats-client --from-literal=password=nats-password -n opni
kubectl describe secret cluster-nats-client -n opni
sleep 90
kubectl apply -f 20_cluster.yaml
rm 20_cluster.yaml

sleep 90
sonobuoy run \
--kubeconfig ~/.kube/config \
--namespace "opni-sono" \
--plugin https://raw.githubusercontent.com/rancher/opni-preprocessing-service/c628170a2f67442d7661866252c90eff78b20806/preprocessing-service/tests/sonobuoy/opnisono-plugin.yaml
for i in {1..60}; do sonobuoy retrieve -n opni-sono && break || sleep 5; done
sonobuoy status -n opni-sono
sonobuoy delete --wait
kubectl delete namespace opni-sono

# kubectl delete -f https://github.com/jetstack/cert-manager/releases/download/v1.5.4/cert-manager.yaml
# kubectl delete -f https://raw.githubusercontent.com/rancher/opni/main/deploy/manifests/00_crds.yaml
# kubectl delete -f https://raw.githubusercontent.com/rancher/opni/main/deploy/manifests/01_rbac.yaml
# kubectl delete -f https://raw.githubusercontent.com/rancher/opni/main/deploy/manifests/10_operator.yaml
# kubectl delete -f 20_cluster.yaml
# kubectl delete namespace opni