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

import org.apache.giraph.comm.RequestRegistry.Type;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.log4j.Logger;
import org.jboss.netty.channel.ChannelHandlerContext;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Reply from server to client for anything other than SASL tokens.
 * TODO: remove parametric type specifiers: not needed here.
 *
 * @param <I> Vertex id
 * @param <V> Vertex data
 * @param <E> Edge data
 * @param <M> Message data
 */
@SuppressWarnings("rawtypes")
public class SaslComplete<I extends WritableComparable,
    V extends Writable, E extends Writable,
    M extends Writable> extends WritableRequest<I, V, E, M> {
  /** Class logger */
  private static final Logger LOG =
      Logger.getLogger(SaslComplete.class);

  private static final Byte zeroByte = 0;

  @Override
  public Type getType() {
    return Type.SASL_COMPLETE;
  }
  /**
   * Constructor used for reflection and sending.
   */
  public SaslComplete() { }

  @Override
  public void readFieldsRequest(DataInput input) throws IOException {
    LOG.debug("read fields (none)");
    byte zeroByte = input.readByte();
  }

  @Override
  public void writeRequest(DataOutput output) throws IOException {
    LOG.debug("writing one byte (write).");
    output.writeByte(zeroByte);
  }

  @Override
  public void doRequest(ServerData<I, V, E, M> serverData,
                        ChannelHandlerContext ctx) {
    if (ctx == null) {
      LOG.debug("ChannelHandlerContext is null: assuming local.");
      return;
    }
    // TODO: replace with nullReply(ctx);
    if (ctx != null) {
      LOG.debug("writing one byte (doRequest).");
      ctx.getChannel().write(this);
    }
    return;
  }
}
