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
# echo "Creating NATs Directory"
# mkdir -p /var/nats
# touch /var/nats/seed
# echo "First Listing"
# ls /var/nats
# echo "Catting File"
# echo '-----BEGIN USER NKEY SEED-----' >> /var/nats/seed
# echo 'SUAHXK3QYQ67HDXAI3HHCZNZLADXKZUF54RNGRZJTQHXCUADCIKMPWHXME' >> /var/nats/seed
# echo '------END USER NKEY SEED------' >> /var/nats/seed 

# echo "Second Listing"
# ls /var/nats
# cat /var/nats/seed
pwd && ls -al
pytest -v -s /src/preprocessing-service/tests/integration/smoke_tests.py --junit-xml=report.xml
cp  report.xml "${results_dir}/report.xml"