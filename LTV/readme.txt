////////////////////////////////////////////////
mvn package in /LTV
must run in ~/git/hadoop/mapreduce/LTV/target

ubuntu@domU-12-31-39-10-48-22:~$ export HADOOP_CLASSPATH=/home/ubuntu/avro-mapred-1.7.6-hadoop2.jar
ubuntu@domU-12-31-39-10-48-22:~$ date=2014-05-20
ubuntu@domU-12-31-39-10-48-22:~$ hadoop fs -rmr /tmp/ltv/d=$date
rmr: DEPRECATED: Please use 'rm -r' instead.
rmr: `/tmp/ltv/d=2014-05-20': No such file or directory

//1 day's data
ubuntu@domU-12-31-39-10-48-22:~$ hadoop jar LTV-mapreduce-1.0-SNAPSHOT.jar com.deepforest.mapreduce.LTV -libjars /home/ubuntu/avro-mapred-1.7.6-hadoop2.jar /flume/attribution-server/ATTRIBUTION/d=${date}/*/* /tmp/ltv/d=${date}

//7 days' data
hadoop jar LTV-mapreduce-1.0-SNAPSHOT.jar com.deepforest.mapreduce.LTV -libjars /home/ubuntu/avro-mapred-1.7.6-hadoop2.jar /flume/attribution-server/ATTRIBUTION/d=2014-05-2[0-7]/*/* /tmp/ltv/d=${date}

//get file from hdfs to cluster
hadoop fs -get hdfs:/tmp/ltv/d=2014-05-20/p* testresult

hadoop fs -get hdfs:/flume/attribution-server/ATTRIBUTION/d=2014-05-20 data

//////////////////////////////////////////////