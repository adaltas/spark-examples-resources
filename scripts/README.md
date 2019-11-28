## Scripts

One can find all the scripts referenced throughout the articles in this folder.

To run one of these application in Spark : 

```bash
<SPARK-PATH>/bin/spark-submit --master yarn \
  --deploy-mode cluster \
  scripts_countTrip/query_4executors.py \
  <Path_to_your_HDFS_Or_Local_file_>
```
