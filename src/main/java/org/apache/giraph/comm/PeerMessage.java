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
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.log4j.Logger;
import org.jboss.netty.buffer.ChannelBufferOutputStream;
import org.jboss.netty.buffer.ChannelBuffers;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.MessageEvent;

import javax.security.sasl.SaslException;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Send and receive SASL tokens.
 *
 * @param <I> Vertex id
 * @param <V> Vertex data
 * @param <E> Edge data
 * @param <M> Message data
 */
@SuppressWarnings("rawtypes")
abstract public class PeerMessage<I extends WritableComparable,
    V extends Writable, E extends Writable,
    M extends Writable> implements WritableRequest<I, V, E, M> {
  /** Class logger */
  private static final Logger LOG =
      Logger.getLogger(PeerMessage.class);
  /** Configuration */
  protected Configuration conf;

  /** Holds the place of the message length until known */
  protected static final byte[] LENGTH_PLACEHOLDER = new byte[4];

  @Override
  abstract public Type getType();

  /**
   * Constructor used for reflection only
   */
  public PeerMessage() { }

  @Override
  abstract public void readFieldsRequest(DataInput input) throws IOException;

  @Override
  abstract public void writeRequest(DataOutput output) throws IOException;

  @Override
  abstract public void doRequest(ServerData<I, V, E, M> serverData,
                                 ChannelHandlerContext ctx);

  @Override
  public Configuration getConf() {
    return conf;
  }

  protected void nullReply(ChannelHandlerContext ctx) {
    if (ctx != null) {
      ctx.getChannel().write(new NullReply<I,V,E,M>());
    }
  }

  @Override
  public void setConf(Configuration conf) {
    this.conf = conf;
  }
}
