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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.log4j.Logger;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBufferInputStream;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelEvent;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.MessageEvent;
import org.jboss.netty.channel.WriteCompletionEvent;
import org.jboss.netty.handler.codec.oneone.OneToOneDecoder;

import java.io.IOException;

/**
 * Decodes encoded responses from the server.
 *
 * @param <I> Vertex id
 * @param <V> Vertex data
 * @param <E> Edge data
 * @param <M> Message data
 */
@SuppressWarnings("rawtypes")
public class ResponseDecoder<I extends WritableComparable,
    V extends Writable, E extends Writable,
    M extends Writable> extends OneToOneDecoder {
  /** Class logger */
  private static final Logger LOG =
      Logger.getLogger(ResponseDecoder.class);
  /** Configuration */
  private final Configuration conf;
  /** Registry of requests */
  private final RequestRegistry requestRegistry;

  /**
   * Constructor.
   *
   * @param conf Configuration
   * @param requestRegistry registry
   */
  public ResponseDecoder(Configuration conf, RequestRegistry requestRegistry) {
    LOG.debug("creating responseDecoder with registry of size:" + requestRegistry.size());
    this.conf = conf;
    this.requestRegistry = requestRegistry;
  }

  /**
   * Used by the client to decode a response from the server.
   */
  @Override
  protected Object decode(ChannelHandlerContext ctx,
      Channel channel, Object msg) throws Exception {
    LOG.debug("RESPONSEDECODER: STARTED; MSG:" + msg);
    if (!(msg instanceof ChannelBuffer)) {
      throw new IllegalStateException("decode: Got illegal message " + msg);
    }

    // Decode msg into an object whose class C implements WritableRequest:
    // (C is one of {SendPartitionMessage,SendPartitionMutation,...SaslTokenMessage})
    //
    // 1. Convert message to a stream that can be decoded.
    ChannelBuffer buffer = (ChannelBuffer) msg;
    ChannelBufferInputStream inputStream = new ChannelBufferInputStream(buffer);
    // 2. Get first byte: message type: one of {SendPartitionMessage, ...., SaslTokenMessage}..
    int enumValue = inputStream.readByte();
    RequestRegistry.Type type = RequestRegistry.Type.values()[enumValue];
    if (LOG.isDebugEnabled()) {
      LOG.debug("decode: Got a response of type " + type + " from server:" +
          channel.getRemoteAddress());
    }

    // 3. create object of this type, into which we will deserialize 
    // the inputStream's data.
    @SuppressWarnings("unchecked")
    Class<? extends WritableRequest<I, V, E, M>> writableRequestClass =
        (Class<? extends WritableRequest<I, V, E, M>>)
        requestRegistry.getClass(type);

    LOG.debug("writableRequestClass: " + writableRequestClass + " found for type:" + type);

    WritableRequest<I, V, E, M> serverResponse =
        ReflectionUtils.newInstance(writableRequestClass, conf);


    LOG.debug("reading fields of server response:" + serverResponse);
    // 4. deserialize the object from the inputStream into serverResponse.
    try {
      serverResponse.readFields(inputStream);
    } catch (IOException e) {
      LOG.error("GOT AN IOException WHEN TRYING TO READ SERVER RESPONSE: " + e);
    }
    LOG.debug("finished reading fields of server response:" + serverResponse);

    // serverResponse can now be used in the next stage in pipeline.
    return serverResponse;
  }
}
