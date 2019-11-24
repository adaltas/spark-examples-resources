## Spark in Yarn : Introduction and application processing

The purpose of this repository is to store the different scripts presented in my article which introduces and illustrates some of the internal aspects of the Spark Engine. All the scripts have been executed using YARN but it should be possible to run them with Mesos or the standalone deployment mode.

For the sake of clarity, the scripts have been splitted into two folders. To run the script, you have to provide your own file path - you can use the template below. 

To be able to run to those Spark scripts using YARN - don't forget to specify your own HDFS or local file path: 

```bash
# clone the repository into your machine 
git clone https://github.com/adaltas/spark-examples-resources.git

# access the new folder
cd spark-examples-resources

# launch the application of your choice, for example: 
<SPARK-PATH>/bin/spark-submit --master yarn \
  --deploy-mode cluster \
  scripts_countTrip/query_4executors.py \
  <Path_to_your_HDFS_Or_Local_file_>
```

Lastly, one can compare the stages duration of the final query written in Scala and Python in the folder `scala-maven-jar`. To be able to run Scala application, you will have to run `mvn package assembly:single` at the root of the Maven project - which is `~/spark-examples-resources/cala-maven-jar/maven/`.
