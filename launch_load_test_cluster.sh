#!/usr/bin/env bash
set -euo pipefail
IFS=$'\n\t'


if [ ! -d "aws-deploy" ]; then
    echo "Warning: aws-deploy not found (run this from the rama project directory)" >&2
    exit 1
fi

NUM=${1:-1}
TOTALNUM=$((NUM + 2))


echo "Configureing aws-deploy..."
cat > aws-deploy/rama.tfvars << EOF
supervisor_num_nodes = ${TOTALNUM}
zookeeper_num_nodes = 1
client_num_nodes = 0
EOF

echo "Starting cluster..."
aws-deploy/bin/rama-cluster.sh deploy hugo-rtree-test
echo "To destroy the cluster: aws-deploy/bin/rama-cluster.sh destroy hugo-rtree-test"

wait_for_http() {
    local url=$1
    local timeout=${2:-30}

    for i in $(seq 1 $timeout); do
	if curl -f -s "$url" > /dev/null; then
	    echo "✓ $url is responding"
	    return 0
	fi
	sleep 1
    done

    echo "✗ Timeout waiting for $url"
    return 1
}

wait_for_port() {
    local host=$1
    local port=$2
    local timeout=${3:-30}

    echo "Waiting for $host:$port..."
    for i in $(seq 1 $timeout); do
	if nc -z "$host" "$port" 2>/dev/null; then
	    echo "✓ port is ready!"
	    return 0
	fi
	sleep 1
    done

    echo "✗ Timeout waiting for port"
    return 1
}


# Usage
CONDUCTOR=$(yq '."conductor.host".internal' ~/.rama/hugo-rtree-test/rama.yaml)

echo "Waitign for port..." # so that the config exists
wait_for_port "${CONDUCTOR}" "1973" 600

echo "Reconfiguring conductor isolation mode..."
scp ${CONDUCTOR}:/data/ec2-user/rama/rama.yaml conductor-rama.yaml.in
sed '/conductor\.assignment\.mode:/,$d' conductor-rama.yaml.in > conductor-rama.yaml
cat >> conductor-rama.yaml <<EOF
conductor.assignment.mode:
  type: isolation
  modules:
    com.rpl.rama.helpers.spatial.loadtest.LoadTest\$SpatialModule: ${NUM}
    com.rpl.rama.helpers.spatial.loadtest.LoadTest\$Module: 1
    monitoring: 1
EOF

scp conductor-rama.yaml ${CONDUCTOR}:/data/ec2-user/rama/rama.yaml

echo "Restarting conductor..."
ssh ${CONDUCTOR} "sudo systemctl restart conductor.service"

echo "Waiting for conductor..."
wait_for_http "http://${CONDUCTOR}:8888" 600
wait_for_port "${CONDUCTOR}" "1973" 600

echo "Conductor responding"

say "Load test cluster is up"
