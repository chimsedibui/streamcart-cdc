#!/bin/sh
# SeaweedFS in one process (master, volume, filer, S3 on 8333) with one S3 identity, and the warehouse bucket for
# Iceberg created once; the healthcheck waits for the marker file. Same setup as the Simulated project's cold store.
# Volumes are 1 GB with a fixed count: the defaults (30 GB each, count sized from free disk) leave none writable on a
# small disk.
set -eu
cat > /tmp/s3.json <<JSON
{"identities": [{"name": "lakehouse",
                 "credentials": [{"accessKey": "$S3_ACCESS_KEY", "secretKey": "$S3_SECRET_KEY"}],
                 "actions": ["Admin", "Read", "Write", "List", "Tagging"]}]}
JSON
weed server -dir=/data -ip=seaweedfs -ip.bind=0.0.0.0 -master.volumeSizeLimitMB=1024 -volume.max=50 -s3 -s3.port=8333 -s3.config=/tmp/s3.json &
server=$!
until echo "s3.bucket.list" | weed shell -master=seaweedfs:9333 >/dev/null 2>&1; do sleep 1; done
echo "s3.bucket.create -name warehouse" | weed shell -master=seaweedfs:9333 >/dev/null 2>&1 || true
touch /data/.bucket-ready
wait $server
