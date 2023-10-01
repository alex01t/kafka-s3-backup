
This is helm chart to backup and restore kafka topic partition.

Unlike kafka-connect-s3 plugins, this one does encode/decode messages with simple base64 and packs batches in json.

By default it creates one s3-object every ~1min or 100k messages. 

It also preserves exact messages' offsets on restore
