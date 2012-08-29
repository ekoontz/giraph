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

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.jboss.netty.channel.ChannelHandlerContext;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Reply from server to client for anything other than SASL tokens.
 * TODO: remove parametric type specifiers if possible since they're not
 * used here.
 *
 * @param <I> Vertex id
 * @param <V> Vertex data
 * @param <E> Edge data
 * @param <M> Message data
 */
@SuppressWarnings("rawtypes")
public class NullReply<I extends WritableComparable,
    V extends Writable, E extends Writable,
    M extends Writable> extends WritableRequest<I, V, E, M> {
  private int workerId;
  private long requestId;
  private int alreadyDone;

  @Override
  public RequestType getType() {
    return RequestType.NULL_REPLY;
  }
  /**
   * Constructor used for reflection and sending.
   */
  public NullReply() {
  }

  public void setWorkerId(int workerId) {
    this.workerId = workerId;
  }
  public void setRequestId(long requestId) {
    this.requestId = requestId;
  }
  public void setAlreadyDone(int alreadyDone) {
    this.alreadyDone = alreadyDone;
  }

  public int getWorkerId() {
    return workerId;
  }

  public long getRequestId() {
    return requestId;
  }

  public int getAlreadyDone() {
    return alreadyDone;
  }

  @Override
  public void readFieldsRequest(DataInput input) throws IOException {
    workerId = input.readInt();
    requestId = input.readLong();
    alreadyDone = input.readByte();
  }

  @Override
  public void writeRequest(DataOutput output) throws IOException {
    output.writeInt(workerId);
    output.writeLong(requestId);
    output.writeByte(alreadyDone);
  }

  @Override
  public void doRequest(ServerData<I, V, E, M> serverData,
                        ChannelHandlerContext ctx) {
    if (ctx == null) {
      return;
    }
    if (ctx != null) {
      ctx.getChannel().write(this);
    }
    return;
  }
}
