/**
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

package org.apache.giraph.comm.netty;

import org.apache.giraph.comm.requests.SaslTokenMessage;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.ipc.RemoteException;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.security.TokenCache;
import org.apache.hadoop.mapreduce.security.token.JobTokenIdentifier;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.SaslRpcServer;
import org.apache.hadoop.security.SaslRpcServer.AuthMethod;
import org.apache.hadoop.security.SaslRpcServer.SaslStatus;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.TokenIdentifier;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.log4j.Logger;

import javax.security.auth.callback.Callback;
import javax.security.auth.callback.CallbackHandler;
import javax.security.auth.callback.NameCallback;
import javax.security.auth.callback.PasswordCallback;
import javax.security.auth.callback.UnsupportedCallbackException;
import javax.security.sasl.RealmCallback;
import javax.security.sasl.RealmChoiceCallback;
import javax.security.sasl.Sasl;
import javax.security.sasl.SaslClient;
import javax.security.sasl.SaslException;
import java.io.DataInputStream;
import java.io.IOException;

/**
 * A utility class that encapsulates SASL logic for Giraph BSPWorker client
 */
public class SaslNettyClient {
  public static final Logger LOG = Logger.getLogger(SaslNettyClient.class);

  public SaslClient saslClient;

  public Object authenticated = new Object();

  /**
   * Create a SaslNettyClient for authentication with BSP servers.
   */
  public SaslNettyClient() {
    try {
      Token<? extends TokenIdentifier> token = createJobToken(new Configuration());
      if (LOG.isDebugEnabled()) {
        LOG.debug("Creating SASL " + AuthMethod.DIGEST.getMechanismName()
            + " client to authenticate to service at " + token.getService());
      }
      saslClient = Sasl.createSaslClient(new String[] { AuthMethod.DIGEST
          .getMechanismName() }, null, null, SaslRpcServer.SASL_DEFAULT_REALM,
          SaslRpcServer.SASL_PROPS, new SaslClientCallbackHandler(token));
    } catch (IOException e) {
      LOG.error("could not obtain job token for Netty Client to use to " +
          "authenticate with a Netty Server.");
      saslClient = null;
    }
  }

  /**
   * Obtain JobToken, which we'll use as a credential for SASL authentication
   * when connecting to other Giraph BSPWorkers.
   */
  private Token<JobTokenIdentifier> createJobToken(Configuration conf)
      throws IOException {
    String localJobTokenFile = System.getenv().get(
        UserGroupInformation.HADOOP_TOKEN_FILE_LOCATION);
    if (localJobTokenFile != null) {
      JobConf jobConf = new JobConf(conf);
      Credentials credentials =
          TokenCache.loadTokens(localJobTokenFile, jobConf);
      return TokenCache.getJobToken(credentials);
    } else {
      throw new IOException("Cannot obtain authentication credentials for " +
          "job: " +
          "file: '" + UserGroupInformation.HADOOP_TOKEN_FILE_LOCATION +
          "' not found");
    }
  }

  public SaslTokenMessage firstToken()
      throws IOException {
    byte[] saslToken = new byte[0];
    if (saslClient.hasInitialResponse()) {
      saslToken = saslClient.evaluateChallenge(saslToken);
    }
    SaslTokenMessage saslTokenMessage =
        new SaslTokenMessage();
    saslTokenMessage.token = saslToken;
    return saslTokenMessage;
  }

  public boolean isComplete() {
    return saslClient.isComplete();
  }

  public String getQOP() {
    String qop = (String)saslClient.getNegotiatedProperty(Sasl.QOP);
    return qop;
  }

  private static void readStatus(DataInputStream inStream) throws IOException {
    int status = inStream.readInt(); // read status
    if (status != SaslStatus.SUCCESS.state) {
      throw new RemoteException(WritableUtils.readString(inStream),
          WritableUtils.readString(inStream));
    }
  }

  public byte[] saslResponse(byte[] saslToken) {
    try {
      byte[] retval = saslClient.evaluateChallenge(saslToken);
      return retval;
    } catch (SaslException e) {
      LOG.error("failed to respond to SASL server's token:" + e);
      return null;
    }
  }

  private static class SaslClientCallbackHandler implements CallbackHandler {
    private final String userName;
    private final char[] userPassword;

    public SaslClientCallbackHandler(Token<? extends TokenIdentifier> token) {
      this.userName = SaslNettyServer.encodeIdentifier(token.getIdentifier());
      this.userPassword = SaslNettyServer.encodePassword(token.getPassword());
    }

    public void handle(Callback[] callbacks)
        throws UnsupportedCallbackException {
      NameCallback nc = null;
      PasswordCallback pc = null;
      RealmCallback rc = null;
      for (Callback callback : callbacks) {
        if (callback instanceof RealmChoiceCallback) {
          continue;
        } else if (callback instanceof NameCallback) {
          nc = (NameCallback) callback;
        } else if (callback instanceof PasswordCallback) {
          pc = (PasswordCallback) callback;
        } else if (callback instanceof RealmCallback) {
          rc = (RealmCallback) callback;
        } else {
          throw new UnsupportedCallbackException(callback,
              "Unrecognized SASL client callback");
        }
      }
      if (nc != null) {
        if (LOG.isDebugEnabled())
          LOG.debug("SASL client callback: setting username: " + userName);
        nc.setName(userName);
      }
      if (pc != null) {
        if (LOG.isDebugEnabled())
          LOG.debug("SASL client callback: setting userPassword");
        pc.setPassword(userPassword);
      }
      if (rc != null) {
        if (LOG.isDebugEnabled())
          LOG.debug("SASL client callback: setting realm: "
              + rc.getDefaultText());
        rc.setText(rc.getDefaultText());
      }
    }
  }
}
