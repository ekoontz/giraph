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

package org.apache.giraph.comm.netty.handler;

import org.apache.giraph.comm.requests.MasterRequest;
import org.apache.hadoop.conf.Configuration;

/** Handler for requests on master */
public class MasterSaslServerHandler extends
    SaslServerHandler<MasterRequest> {
  /**
   * Constructor
   *
   * @param workerRequestReservedMap Worker request reservation map
   * @param conf                     Configuration
   */
  public MasterSaslServerHandler(
    WorkerRequestReservedMap workerRequestReservedMap,
    Configuration conf) {
    // note that first param, (ServerData) to super's constructor is null.
    super(null, workerRequestReservedMap, conf);
  }

  @Override
  public void processRequest(MasterRequest request) {
    request.doRequest();
  }

  /**
   * Factory for {@link MasterSaslServerHandler}
   */
  public static class Factory implements SaslServerHandler.Factory {
    @Override
    public SaslServerHandler newHandler(
        WorkerRequestReservedMap workerRequestReservedMap, Configuration conf) {
      return new MasterSaslServerHandler(workerRequestReservedMap, conf);
    }
  }
}
