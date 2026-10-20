#!/usr/bin/env bash
# Creates the development cluster's CA and one certificate per Elasticsearch node, once.
#
# Runs as root in the Elasticsearch image (it ships elasticsearch-certutil). The certs volume is
# shared read-only by es01..es03; the es-ca volume receives only the public CA certificate, which
# is all a client (the pipeline, es-users) needs to verify the nodes. Private keys never leave
# the certs volume. Rerunning is a no-op once the files exist, so `docker compose up` after a
# restart keeps the same CA.
set -euo pipefail

home=/usr/share/elasticsearch
certs="$home/config/certs"
public=/ca-public
nodes=(es01 es02 es03)

cd "$home"

if [[ ! -f "$certs/ca/ca.crt" ]]; then
  echo "certs: creating the certificate authority"
  bin/elasticsearch-certutil ca --silent --pem --out "$certs/ca.zip"
  unzip -q -o "$certs/ca.zip" -d "$certs"
  rm -f "$certs/ca.zip"
fi

missing=0
for node in "${nodes[@]}"; do
  [[ -f "$certs/$node/$node.crt" ]] || missing=1
done

if [[ "$missing" -eq 1 ]]; then
  echo "certs: creating node certificates for ${nodes[*]}"
  {
    echo "instances:"
    for node in "${nodes[@]}"; do
      # localhost and 127.0.0.1 let a client on the host reach es01 through the published port
      # with full hostname verification.
      printf '  - name: %s\n    dns: [%s, localhost]\n    ip: [127.0.0.1]\n' "$node" "$node"
    done
  } >"$certs/instances.yml"
  bin/elasticsearch-certutil cert --silent --pem \
    --in "$certs/instances.yml" \
    --out "$certs/nodes.zip" \
    --ca-cert "$certs/ca/ca.crt" \
    --ca-key "$certs/ca/ca.key"
  unzip -q -o "$certs/nodes.zip" -d "$certs"
  rm -f "$certs/nodes.zip"
fi

# Elasticsearch runs as uid 1000 with gid 0: group read is enough, and others get nothing.
chown -R root:root "$certs"
find "$certs" -type d -exec chmod 750 {} +
find "$certs" -type f -exec chmod 640 {} +

# The public CA for clients, world-readable (the pipeline runs as uid 65532).
install -m 0644 "$certs/ca/ca.crt" "$public/ca.crt"
echo "certs: ready"
