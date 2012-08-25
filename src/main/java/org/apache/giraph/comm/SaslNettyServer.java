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
// copied from http://docs.jboss.org/netty/3.2/api/org/jboss/netty/handler/queue/BufferedWriteHandler.html
package org.apache.giraph.comm;

import org.apache.commons.net.util.Base64;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.StandbyException;
import org.apache.hadoop.mapreduce.security.token.JobTokenIdentifier;
import org.apache.hadoop.mapreduce.security.token.JobTokenSecretManager;
import org.apache.hadoop.security.SaslRpcServer;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.UserGroupInformation.AuthenticationMethod;
import org.apache.hadoop.security.token.SecretManager;
import org.apache.hadoop.security.token.SecretManager.InvalidToken;
import org.apache.hadoop.security.token.TokenIdentifier;
import org.apache.log4j.Logger;

import javax.security.auth.callback.Callback;
import javax.security.auth.callback.CallbackHandler;
import javax.security.auth.callback.NameCallback;
import javax.security.auth.callback.PasswordCallback;
import javax.security.auth.callback.UnsupportedCallbackException;
import javax.security.sasl.AuthorizeCallback;
import javax.security.sasl.RealmCallback;
import javax.security.sasl.Sasl;
import javax.security.sasl.SaslException;
import javax.security.sasl.SaslServer;
import java.io.ByteArrayInputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Map;
import java.util.TreeMap;

/**
 * A utility class for dealing with SASL on Giraph.
 */
public class SaslNettyServer {
  public static final Logger LOG = Logger.getLogger(SaslNettyServer.class);
  public static final String SASL_DEFAULT_REALM = "default";
  public static final Map<String, String> SASL_PROPS = 
      new TreeMap<String, String>();

  public static final int SWITCH_TO_SIMPLE_AUTH = -88;

  public static enum QualityOfProtection {
    AUTHENTICATION("auth"),
    INTEGRITY("auth-int"),
    PRIVACY("auth-conf");
    
    public final String saslQop;
    
    private QualityOfProtection(String saslQop) {
      this.saslQop = saslQop;
    }
    
    public String getSaslQop() {
      return saslQop;
    }
  }

  private SaslServer saslServer;

  public SaslNettyServer(JobTokenSecretManager secretManager) {
    LOG.debug("creating saslNettyServer: secret manager is: " + secretManager);
    try {
      secretManager.checkAvailableForRead();
    } catch (StandbyException e) {
      LOG.error("could not read secret manager: " + e);
    }
    LOG.debug("checked read-availability; now creating sasl server.");
    try {
      LOG.debug("create...");

      LOG.debug("creating callback handler..");
      SaslDigestCallbackHandler ch = new SaslNettyServer.SaslDigestCallbackHandler(secretManager);
      LOG.debug("created callback handler: " + ch);


      saslServer = Sasl.createSaslServer(SaslNettyServer.AuthMethod.DIGEST
          .getMechanismName(), null, SaslRpcServer.SASL_DEFAULT_REALM,
          SaslRpcServer.SASL_PROPS, ch);
      LOG.debug("create was successful.");
    } catch (SaslException e) {
      LOG.error("Could not create SaslServer: " + e);
    }
  }

  public boolean isComplete() {
    return saslServer.isComplete();
  }

  public String getUserName() {
    return saslServer.getAuthorizationID();
  }

  public byte[] response(byte[] token) {
    try {
      LOG.debug("responding to input token of length: " + token.length);
      byte[] retval = saslServer.evaluateResponse(token);
      LOG.debug("response token length: " + retval.length);
      return retval;
    } catch (SaslException e) {
      LOG.error("failed to evaluate client token of length: " + token.length + " : " + e);
      return null;
    }
  }

  public static void init(Configuration conf) {
    LOG.debug("started static initialization");
    QualityOfProtection saslQOP = QualityOfProtection.AUTHENTICATION;
    String rpcProtection = conf.get("hadoop.rpc.protection",
        QualityOfProtection.AUTHENTICATION.name().toLowerCase());
    if (QualityOfProtection.INTEGRITY.name().toLowerCase()
        .equals(rpcProtection)) {
      saslQOP = QualityOfProtection.INTEGRITY;
    } else if (QualityOfProtection.PRIVACY.name().toLowerCase().equals(
        rpcProtection)) {
      saslQOP = QualityOfProtection.PRIVACY;
    }
    
    SASL_PROPS.put(Sasl.QOP, saslQOP.getSaslQop());
    SASL_PROPS.put(Sasl.SERVER_AUTH, "true");
    LOG.debug("done with static initialization");
  }
  
  static String encodeIdentifier(byte[] identifier) {
    return new String(Base64.encodeBase64(identifier));
  }

  static byte[] decodeIdentifier(String identifier) {
    return Base64.decodeBase64(identifier.getBytes());
  }

  public static <T extends TokenIdentifier> T getIdentifier(String id,
      SecretManager<T> secretManager) throws InvalidToken {
    byte[] tokenId = decodeIdentifier(id);
    T tokenIdentifier = secretManager.createIdentifier();
    try {
      tokenIdentifier.readFields(new DataInputStream(new ByteArrayInputStream(
          tokenId)));
    } catch (IOException e) {
      throw (InvalidToken) new InvalidToken(
          "Can't de-serialize tokenIdentifier").initCause(e);
    }
    return tokenIdentifier;
  }

  static char[] encodePassword(byte[] password) {
    return new String(Base64.encodeBase64(password)).toCharArray();
  }

  /** Splitting fully qualified Kerberos name into parts */
  public static String[] splitKerberosName(String fullName) {
    return fullName.split("[/@]");
  }

  @InterfaceStability.Evolving
  public enum SaslStatus {
    SUCCESS (0),
    ERROR (1);
    
    public final int state;
    private SaslStatus(int state) {
      this.state = state;
    }
  }
  
  /** Authentication method */
  @InterfaceStability.Evolving
  // TODO: remove SIMPLE and KERBEROS since we won't be using them with Giraph.
  public static enum AuthMethod {
    SIMPLE((byte) 80, "", AuthenticationMethod.SIMPLE),
    KERBEROS((byte) 81, "GSSAPI", AuthenticationMethod.KERBEROS),
    DIGEST((byte) 82, "DIGEST-MD5", AuthenticationMethod.TOKEN);

    /** The code for this method. */
    public final byte code;
    public final String mechanismName;
    public final AuthenticationMethod authenticationMethod;

    private AuthMethod(byte code, String mechanismName, 
                       AuthenticationMethod authMethod) {
      this.code = code;
      this.mechanismName = mechanismName;
      this.authenticationMethod = authMethod;
    }

    private static final int FIRST_CODE = values()[0].code;

    /** Return the object represented by the code. */
    private static AuthMethod valueOf(byte code) {
      final int i = (code & 0xff) - FIRST_CODE;
      return i < 0 || i >= values().length ? null : values()[i];
    }

    /** Return the SASL mechanism name */
    public String getMechanismName() {
      return mechanismName;
    }

    /** Read from in */
    public static AuthMethod read(DataInput in) throws IOException {
      return valueOf(in.readByte());
    }

    /** Write to out */
    public void write(DataOutput out) throws IOException {
      out.write(code);
    }
  };

  /** CallbackHandler for SASL DIGEST-MD5 mechanism */
  @InterfaceStability.Evolving
  public static class SaslDigestCallbackHandler implements CallbackHandler {
    private JobTokenSecretManager secretManager;

    public SaslDigestCallbackHandler(
        JobTokenSecretManager secretManager) {
      LOG.debug("creating SaslDigestCallback handler with secret manager: " + secretManager);
      this.secretManager = secretManager;
    }

    private char[] getPassword(JobTokenIdentifier tokenid) throws InvalidToken {
      byte[] retrievedPassword = secretManager.retrievePassword(tokenid);
      if (retrievedPassword == null) {
        LOG.error("could not get password.");
      }
      return encodePassword(retrievedPassword);
    }

    /** {@inheritDoc} */
    @Override
    public void handle(Callback[] callbacks) throws InvalidToken,
        UnsupportedCallbackException {
      NameCallback nc = null;
      PasswordCallback pc = null;
      AuthorizeCallback ac = null;
      for (Callback callback : callbacks) {
        if (callback instanceof AuthorizeCallback) {
          ac = (AuthorizeCallback) callback;
        } else if (callback instanceof NameCallback) {
          nc = (NameCallback) callback;
        } else if (callback instanceof PasswordCallback) {
          pc = (PasswordCallback) callback;
        } else if (callback instanceof RealmCallback) {
          continue; // realm is ignored
        } else {
          throw new UnsupportedCallbackException(callback,
              "Unrecognized SASL DIGEST-MD5 Callback");
        }
      }
      if (pc != null) {
        JobTokenIdentifier tokenIdentifier = getIdentifier(nc.getDefaultName(), secretManager);
        char[] password = getPassword(tokenIdentifier);
        UserGroupInformation user = null;
        user = tokenIdentifier.getUser(); // may throw exception

        if (LOG.isDebugEnabled()) {
          LOG.debug("SASL server DIGEST-MD5 callback: setting password "
              + "for client: " + tokenIdentifier.getUser());
        }
        pc.setPassword(password);
      }
      if (ac != null) {
        String authid = ac.getAuthenticationID();
        String authzid = ac.getAuthorizationID();
        if (authid.equals(authzid)) {
          ac.setAuthorized(true);
        } else {
          ac.setAuthorized(false);
        }
        if (ac.isAuthorized()) {
          if (LOG.isDebugEnabled()) {
            String username =
              getIdentifier(authzid, secretManager).getUser().getUserName();
            LOG.debug("SASL server DIGEST-MD5 callback: setting "
                + "canonicalized client ID: " + username);
          }
          ac.setAuthorizedID(authzid);
        }
      }
    }
  }
}
