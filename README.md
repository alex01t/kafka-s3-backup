
This is helm chart to backup and restore kafka topic partition.

Unkike kafka-connect-s3 plugins, this one does encode/decode messages with simple base64 and packs batches in json.

It also preserves exact messages' offsets