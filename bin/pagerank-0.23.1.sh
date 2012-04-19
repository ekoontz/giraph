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
export HADOOP_HOME=$HOME/hadoop-0.23.1
export HADOOP_CONF_DIR=$HOME/hadoop-0.23.1/etc/hadoop
export MVN_PROFILE=hadoop_0.23
# Extra things could be added to HADOOP_CLASSPATH, if you want, but "" 
# should work fine, at least for the pagerank benchmark.
export HADOOP_CLASSPATH=""
rm -rf _bsp
bin/giraph pagerank -v -w 1 -s 20 -c 0 -e 5 -V 20
