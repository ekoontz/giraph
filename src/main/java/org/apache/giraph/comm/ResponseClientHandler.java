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

import org.apache.giraph.graph.GiraphJob;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.log4j.Logger;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBufferInputStream;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelEvent;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.MessageEvent;
import org.jboss.netty.handler.codec.oneone.OneToOneDecoder;

import java.io.IOException;
import java.util.concurrent.ConcurrentMap;

/**
 * Generic handler of responses.
 */
public class ResponseClientHandler extends OneToOneDecoder {
  /** Class logger */
  private static final Logger LOG =
      Logger.getLogger(ResponseClientHandler.class);
  /** Configuration */
  private final Configuration conf;
  /** Registry of requests */
  private final RequestRegistry requestRegistry;

  /** Already dropped first response? (used if dropFirstResponse == true) */
  private static volatile boolean ALREADY_DROPPED_FIRST_RESPONSE = false;
  /** Drop first response (used for simulating failure) */
  private final boolean dropFirstResponse;
  /** Outstanding worker request map */
  private final ConcurrentMap<ClientRequestId, RequestInfo>
  workerIdOutstandingRequestMap;

  /**
   * Constructor.
   *
   * @param conf Configuration
   * @param requestRegistry registry
   */
  public ResponseClientHandler(Configuration conf, RequestRegistry requestRegistry,
                               ConcurrentMap<ClientRequestId, RequestInfo>
                                   workerIdOutstandingRequestMap) {
    this.conf = conf;
    this.requestRegistry = requestRegistry;
    this.workerIdOutstandingRequestMap = workerIdOutstandingRequestMap;
    dropFirstResponse = conf.getBoolean(
        GiraphJob.NETTY_SIMULATE_FIRST_RESPONSE_FAILED,
        GiraphJob.NETTY_SIMULATE_FIRST_RESPONSE_FAILED_DEFAULT);
  }

  // TODO: add a separate pipeline component here on client-side, called
  // called SaslClientCodec, dedicated to handling SASL messages from servers,
  // rather than here in handleUpstream().
  @Override
  public void handleUpstream(
      ChannelHandlerContext ctx, ChannelEvent evt)
      throws Exception {
    if (!(evt instanceof MessageEvent)) {
      LOG.debug("handleUpstream(): ignoring non-message: " + evt);
      ctx.sendUpstream(evt);
      return;
    }
    MessageEvent e = (MessageEvent)evt;
    Object originalMessage = ((MessageEvent) e).getMessage();
    Object decodedMessage = decode(ctx, ctx.getChannel(), originalMessage);
    LOG.debug("handleUpstream(): originalMessage: " + originalMessage);
    LOG.debug("handleUpstream(): decodedMessage:  " + decodedMessage);
    if (originalMessage == decodedMessage) {
      LOG.debug("original==decoded: sending upstream and returning.");
      ctx.sendUpstream(evt);
      return;
    } else {
      if (decodedMessage != null) {
        if (decodedMessage.getClass() == NullReply.class) {
          NullReply nullReply = (NullReply) decodedMessage;

          int senderId = nullReply.getWorkerId();
          long requestId = nullReply.getRequestId();
          int response = nullReply.getAlreadyDone();

          LOG.debug("handleUpstream(): senderId=" + senderId + "; requestId=" +
            requestId + ")");

          // Simulate a failed response on the first response (if desired)
          if (dropFirstResponse && !ALREADY_DROPPED_FIRST_RESPONSE) {
            LOG.info("handleUpstream(): Simulating dropped response " + response +
              " for request " + requestId);
            ALREADY_DROPPED_FIRST_RESPONSE = true;
            synchronized (workerIdOutstandingRequestMap) {
              workerIdOutstandingRequestMap.notifyAll();
            }
            return;
          }

          if (response == 1) {
            LOG.info("handleUpstream(): Already completed request " + requestId);
          } else if (response != 0) {
            throw new IllegalStateException(
              "handleUpstream(): Got illegal response " + response);
          }

          LOG.debug("decode(): looking up client request id with senderId=" +
            senderId + ", requestId: " + requestId);
          RequestInfo requestInfo = workerIdOutstandingRequestMap.remove(
            new ClientRequestId(senderId, requestId));
          if (requestInfo == null) {
            LOG.error("senderId,requestId:" + senderId + "," + requestId + " is not legitimate.");
            throw new IllegalStateException("handleUpstream(): Impossible to " +
              "have a non-registered requestId " + requestId);
          } else {
            LOG.error("senderId,requestId:" + senderId + "," + requestId + " is legitimate.");
            if (LOG.isDebugEnabled()) {
              LOG.debug("handleUpstream(): Processed request id = " + requestId +
                " " + requestInfo + ".  Waiting on " +
                workerIdOutstandingRequestMap.size() + " requests.");
            }
          }

          // Help NettyClient#waitSomeRequests() to finish faster
          synchronized (workerIdOutstandingRequestMap) {
            workerIdOutstandingRequestMap.notifyAll();
          }

          LOG.debug("ResponseClientHandler is now calling super.handleUpstream().");
          super.handleUpstream(ctx,evt);
          return;
        }

        // generate SASL response to server.
        LOG.debug("original != decoded and decoded is not null:" +
          "considering how to respond to server's SASL-related server message.");

        // get SASL client for this ctx.
        SaslNettyClient saslNettyClient = NettyClient.SASL.get(ctx.getChannel());
        if (saslNettyClient == null) {
          throw new Exception("saslNettyClient was unexpectedly for channel: " +
            ctx.getChannel());
        }
        if (decodedMessage.getClass() == SaslComplete.class) {
          LOG.debug("Server has sent us the SaslComplete message. Notifying " +
            "so that normal work may now proceed.");
          synchronized(saslNettyClient.authenticated) {
            saslNettyClient.authenticated.notify();
          }
          LOG.debug("client's SASL completeness is: " +
            saslNettyClient.isComplete());
          if (saslNettyClient.isComplete() != true) {
            LOG.error("Server returned a Sasl-complete message, but as far " +
              "as we can tell, we are not authenticated yet.");
            throw new Exception("Server returned a Sasl-complete message, " +
              "but as far as we can tell, we are not authenticated yet.");
          }
          return;
        } else {
          if (saslNettyClient.isComplete() == true) {
            LOG.debug("Sasl completion is done. we should be working on " +
              " real work now.");
          }
          LOG.debug("authentication is not finished yet; continuing.");
          if (saslNettyClient.isComplete() == true) {
            LOG.error("Server did not return a Sasl-complete message, " +
              "but as far we can tell, authentication is complete.");
            throw new Exception("Server did not return a Sasl-complete message, " +
              "but as far as we can tell, authentication is complete.");
          }
        }

        SaslTokenMessage serverToken = (SaslTokenMessage)decodedMessage;
        LOG.debug("responding to server's token of length: " +
          serverToken.token.length);
          // generate SASL response.
        byte[] responseToServer = saslNettyClient.saslResponse(serverToken.token);
        if (responseToServer == null) {
          LOG.debug("response to server is null. Hopefully we are done with " +
            "authentication at this point.");
          if (saslNettyClient.isComplete() == true) {
              LOG.debug("SASL authentication is complete: the server should " +
                "send us a SaslComplete message next.");
          } else {
            LOG.error("Not complete, but sasl response is null: that's not " +
              "expected.");
          }
        } else {
          LOG.debug("response to server token has length:" + responseToServer.length);
          // send downstream.
          SaslTokenMessage saslResponse = new SaslTokenMessage(responseToServer);
          ctx.getChannel().write(saslResponse);
        }
      } else {
        LOG.debug("decoded message is null: stopping processing of event: " + evt);
      }
    }
  }

  /**
   * Used by the client to decode a response from the server.
   */
  @Override
  protected Object decode(ChannelHandlerContext ctx,
      Channel channel, Object msg) throws Exception {
    LOG.debug("decode(): decoding server response from source message:" + msg);
    if (!(msg instanceof ChannelBuffer)) {
      throw new IllegalStateException("decode: Got illegal message " + msg);
    }

    // Decode msg into an object whose class C implements WritableRequest:
    // (C is one of {SaslTokenMessage, SaslCompleteMessage, NullReply}.
    //
    // 1. Convert message to a stream that can be decoded.
    ChannelBuffer buffer = (ChannelBuffer) msg;
    ChannelBufferInputStream inputStream = new ChannelBufferInputStream(buffer);

    LOG.debug("decode(): reading from inputStream: " + inputStream);

    // 2. Get first byte: message type:
    // one of {SendPartitionMessage, ..., SaslTokenMessage}.
    int enumValue = inputStream.readByte();
    RequestType type = RequestType.values()[enumValue];
    if (LOG.isDebugEnabled()) {
      LOG.debug("decode(): Got a response of type " + type + " from server:" +
          channel.getRemoteAddress());
    }

    LOG.debug("decode(): now decoding msg of type:" + type +
      " into a WritableRequest.");

    // 3. create object of this type, into which we will deserialize 
    // the inputStream's data.
        Class<? extends WritableRequest> writableRequestClass =
        requestRegistry.getClass(type);

    LOG.debug("decode(): writableRequestClass: " + writableRequestClass +
      " found for type:" + type);

    WritableRequest serverResponse =
        ReflectionUtils.newInstance(writableRequestClass, conf);

    LOG.debug("decode(): reading fields of server response:" + serverResponse);
    // 4. deserialize the object from the inputStream into serverResponse.
    try {
      serverResponse.readFields(inputStream);
    } catch (IOException e) {
      LOG.error("GOT AN IOException WHEN TRYING TO READ SERVER RESPONSE: " + e);
    }
    LOG.debug("decode(): finished reading fields of server response:" +
      serverResponse);

    // serverResponse can now be used in the next stage in pipeline.
    return serverResponse;
  }
}
