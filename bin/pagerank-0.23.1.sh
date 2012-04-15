#!/bin/bash
#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
export HADOOP_HOME=/Users/ekoontz/hadoop-0.23.1
export HADOOP_CONF_DIR=/Users/ekoontz/hadoop-0.23.1/etc/hadoop
export MVN_PROFILE=hadoop_0.23
export HADOOP_CLASSPATH=:/Users/ekoontz/hadoop-0.23.1/etc/hadoop:/Users/ekoontz/hadoop-0.23.1/share/hadoop/common/lib/*:/Users/ekoontz/hadoop-0.23.1/share/hadoop/common/*:::/Users/ekoontz/giraph/conf::/Users/ekoontz/hadoop-0.23.1/share/hadoop/hdfs:/Users/ekoontz/hadoop-0.23.1/share/hadoop/hdfs/lib/*:/Users/ekoontz/hadoop-0.23.1/share/hadoop/hdfs/*:/Users/ekoontz/hadoop-0.23.1/share/hadoop/mapreduce/lib/*:/Users/ekoontz/hadoop-0.23.1/share/hadoop/mapreduce/*:/Users/ekoontz/giraph/conf:
rm -rf _bsp
bin/giraph pagerank -v -w 1 -s 20 -c 0 -e 5 -V 20

#export CLASSPATH=/Users/ekoontz/hadoop-0.23.1/etc/hadoop:/Users/ekoontz/hadoop-0.23.1/share/hadoop/common/lib/*:/Users/ekoontz/hadoop-0.23.1/share/hadoop/common/*:::/Users/ekoontz/giraph/conf::/Users/ekoontz/hadoop-0.23.1/share/hadoop/hdfs:/Users/ekoontz/hadoop-0.23.1/share/hadoop/hdfs/lib/*:/Users/ekoontz/hadoop-0.23.1/share/hadoop/hdfs/*:/Users/ekoontz/hadoop-0.23.1/share/hadoop/mapreduce/lib/*:/Users/ekoontz/hadoop-0.23.1/share/hadoop/mapreduce/*:/Users/ekoontz/giraph/conf:
#java -Xmx1000m -Dhadoop.log.dir=/Users/ekoontz/hadoop-0.23.1/logs -Dhadoop.log.file=hadoop.log -Dhadoop.home.dir=/Users/ekoontz/hadoop-0.23.1 -Dhadoop.id.str= -Dhadoop.root.logger=INFO,console -Dhadoop.policy.file=hadoop-policy.xml -Djava.net.preferIPv4Stack=true -Dhadoop.security.logger=INFO,NullAppender org.apache.hadoop.util.RunJar /Users/ekoontz/giraph/target/giraph-0.2-SNAPSHOT-jar-with-dependencies.jar org.apache.giraph.benchmark.PageRankBenchmark -libjars /Users/ekoontz/giraph/target/giraph-0.2-SNAPSHOT-jar-with-dependencies.jar,/Users/ekoontz/giraph/conf -v -w 1 -s 20 -c 0 -e 5 -V 7
 