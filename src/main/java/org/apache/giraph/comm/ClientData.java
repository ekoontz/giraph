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

package org.apache.giraph.comm;

import org.apache.log4j.Logger;

import java.net.InetSocketAddress;

/**
 * Anything that the client stores
 */
public class ClientData {
  /** TODO: document how this is used. */
  public static final Logger LOG = Logger.getLogger(ClientData.class);

  /** TODO: document how this is used. */
  private NettyClient nettyClient;

  /**
   * Constructor:
   * @param nettyClient used to respond to server responses.
   * TODO: use channel-local storage instead.
   */
  public ClientData(NettyClient nettyClient) {
    this.nettyClient = nettyClient;
  }

  /**
   * TODO: used by ResponseClient handler so client can respond to server
   * responses (such as SASL challenges).
   * @param inetSocketAddress address of server
   * @param request request (actually response to server) to send.
   */
  public void sendWritableRequest(InetSocketAddress inetSocketAddress,
                                  WritableRequest request) {
    nettyClient.sendWritableRequest(inetSocketAddress, request);
  }

}
