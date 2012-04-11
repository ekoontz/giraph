/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.giraph;

import java.io.IOException;

import org.apache.giraph.examples.SimpleCheckpointVertex;
import org.apache.giraph.examples.SimpleSuperstepVertex.SimpleSuperstepVertexOutputFormat;
import org.apache.giraph.examples.SimpleSuperstepVertex.SimpleSuperstepVertexInputFormat;
import org.apache.giraph.graph.GiraphJob;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import junit.framework.Test;
import junit.framework.TestSuite;
import org.apache.log4j.Logger;

/**
 * Unit test for Yarn
 */
public class TestYarn extends BspCase {
  /** Where the checkpoints will be stored and restarted */
  private final String HDFS_CHECKPOINT_DIR =
      "/tmp/testBspYarn";

  /** Class logger */
  private static final Logger LOG = Logger.getLogger(TestYarn.class);

  /**
   * Create the test case
   *
   * @param testName name of the test case
   */
  public TestYarn(String testName) {
    super(testName);
  }

  /**
   * @return the suite of tests being tested
   */
  public static Test suite() {
    return new TestSuite(TestYarn.class);
  }

  /**
   * Run a sample Yarn job.
   * @throws IOException
   * @throws ClassNotFoundException
   * @throws InterruptedException
   */
  public void testYarn()
      throws IOException, InterruptedException, ClassNotFoundException {
    GiraphJob job = new GiraphJob(getCallingMethodName());
    setupConfiguration(job);
    job.getConfiguration().set(GiraphJob.CHECKPOINT_DIRECTORY,
        HDFS_CHECKPOINT_DIR);
    job.getConfiguration().setBoolean(
        GiraphJob.CLEANUP_CHECKPOINTS_AFTER_SUCCESS, false);
    job.setVertexClass(SimpleCheckpointVertex.class);
    job.setWorkerContextClass(
        SimpleCheckpointVertex.SimpleCheckpointVertexWorkerContext.class);
    job.setVertexInputFormatClass(SimpleSuperstepVertexInputFormat.class);
    job.setVertexOutputFormatClass(SimpleSuperstepVertexOutputFormat.class);
    Path outputPath = new Path("/tmp/" + getCallingMethodName());
    removeAndSetOutput(job, outputPath);
    assertTrue(job.run(true));
    long fileLen = 0;
    long idSum = 0;
    if (getJobTracker() == null) {
      FileStatus fileStatus = getSinglePartFileStatus(job, outputPath);
      fileLen = fileStatus.getLen();
      idSum =
          SimpleCheckpointVertex.SimpleCheckpointVertexWorkerContext.getFinalSum();
      System.out.println("testBspCheckpoint: idSum = " + idSum +
          " fileLen = " + fileLen);
    }
  }
}
