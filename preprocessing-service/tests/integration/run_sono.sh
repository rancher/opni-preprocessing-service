#!/usr/bin/env bash

results_dir="${RESULTS_DIR:-/tmp/results}"
junit_report_file="${results_dir}/report.xml"

# saveResults prepares the results for handoff to the Sonobuoy worker.
# See: https://github.com/vmware-tanzu/sonobuoy/blob/master/site/docs/master/plugins.md
saveResults() {
  # Signal to the worker that we are done and where to find the results.
  printf ${junit_report_file} >"${results_dir}/done"
}

# Ensure that we tell the Sonobuoy worker we are done regardless of results.
trap saveResults EXIT

mkdir "${results_dir}" || true

kubectl get secret cluster-nats-client -n opni -o yaml > file.yml
sed -i '4,26d' file.yml
sed -i '1,2d' file.yml
sed -i '/ password: /d'
base64 --decode file.yml > file.yml.b64
export NATS_PASSWORD="${file.yml.b64}"
rm file.yml
rm file.yml.b64

pwd && ls -al
pytest -v -s /src/preprocessing-service/tests/integration/smoke_tests.py --junit-xml=report.xml
cp  report.xml "${results_dir}/report.xml"