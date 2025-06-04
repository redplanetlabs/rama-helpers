#!/usr/bin/env bash

set -euo pipefail
IFS=$'\n\t'

NUM=${1:-1}
NUMTASKS=$((NUM * 4))

# cat > aws-deploy/rama.tfvars <<<EOF
# supervisor_num_nodes = 2
# zookeeper_num_nodes = 1
# client_num_nodes = 0
# EOF
# aws-deploy/bin/rama-cluster.sh deploy hugo-rtree-test
# aws-deploy/bin/rama-cluster.sh destroy hugo-rtree-test

RAMA=rama-hugo-rtree-test


wait_for_module_running() {
    local module="$1"

    if [ -z "$module" ]; then
        echo "Error: Module name is required"
        return 1
    fi

    echo "Waiting for module '$module' to reach RUNNING status..."

    while true; do
        status=$(${RAMA} moduleStatus --useInternalHostnames "$module" | jq -r ".moduleState")

        if [ "$status" = "RUNNING" ]; then
            echo "Module '$module' is now RUNNING"
            return 0
        fi

        echo "Current status: $status - waiting..."
        sleep 5
    done
}


${RAMA} deploy \
	--action launch --systemModule monitoring \
	--workers 1 --tasks 4 --threads 4 --replicationFactor 1 \
	--useInternalHostnames

wait_for_module_running \
    "rpl.rama.distributed.monitoring.module/Monitoring"


${RAMA} deploy \
	--action launch \
	--jar target/rama-helpers-fat-jar-with-tests.jar \
	--module "com.rpl.rama.helpers.spatial.loadtest.LoadTest\$SpatialModule" \
	--workers ${NUM} --tasks ${NUMTASKS} --threads ${NUMTASKS} \
	--useInternalHostnames

wait_for_module_running \
    "com.rpl.rama.helpers.spatial.loadtest.LoadTest\$SpatialModule"

echo -n "Press Enter to deploy load test module: "
read -r

${RAMA} deploy \
	--action launch \
	--jar target/rama-helpers-fat-jar-with-tests.jar \
	--module "com.rpl.rama.helpers.spatial.loadtest.LoadTest\$Module" \
	--workers 1 --tasks 4 --threads 4 \
	--useInternalHostnames
