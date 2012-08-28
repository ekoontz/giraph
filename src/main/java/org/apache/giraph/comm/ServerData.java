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

import org.apache.giraph.comm.messages.MessageStoreByPartition;
import org.apache.giraph.comm.messages.MessageStoreFactory;
import org.apache.giraph.graph.GiraphJob;
import org.apache.giraph.graph.Vertex;
import org.apache.giraph.graph.VertexMutations;
import org.apache.giraph.graph.partition.DiskBackedPartitionStore;
import org.apache.giraph.graph.partition.PartitionStore;
import org.apache.giraph.graph.partition.SimplePartitionStore;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.security.TokenCache;
import org.apache.hadoop.mapreduce.security.token.JobTokenIdentifier;
import org.apache.hadoop.mapreduce.security.token.JobTokenSecretManager;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.TokenIdentifier;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.Collection;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Anything that the server stores
 *
 * @param <I> Vertex id
 * @param <V> Vertex data
 * @param <E> Edge data
 * @param <M> Message data
 */
@SuppressWarnings("rawtypes")
public class ServerData<I extends WritableComparable,
    V extends Writable, E extends Writable, M extends Writable> {
  /** Class logger */
  private static final Logger LOG = Logger.getLogger(ServerData.class);

  /** Partition store for this worker. */
  private volatile PartitionStore<I, V, E, M> partitionStore;
  /** Message store factory */
  private final
  MessageStoreFactory<I, M, MessageStoreByPartition<I, M>> messageStoreFactory;
  /**
   * Message store for incoming messages (messages which will be consumed
   * in the next super step)
   */
  private volatile MessageStoreByPartition<I, M> incomingMessageStore;
  /**
   * Message store for current messages (messages which we received in
   * previous super step and which will be consumed in current super step)
   */
  private volatile MessageStoreByPartition<I, M> currentMessageStore;
  /**
   * Map of partition ids to incoming vertex mutations from other workers.
   * (Synchronized access to values)
   */
  private final ConcurrentHashMap<I, VertexMutations<I, V, E, M>>
  vertexMutations = new ConcurrentHashMap<I, VertexMutations<I, V, E, M>>();
  /**
   * Used to store secret shared with clients so that we can authenticate
   * them.
   */
  public JobTokenSecretManager secretManager = new JobTokenSecretManager();

  /**
   * Constructor.
   *
   * @param configuration Configuration
   * @param messageStoreFactory Factory for message stores
   */
  public ServerData(Configuration configuration,
                    MessageStoreFactory<I, M, MessageStoreByPartition<I, M>>
                        messageStoreFactory) {

    this.messageStoreFactory = messageStoreFactory;
    currentMessageStore = messageStoreFactory.newStore();
    incomingMessageStore = messageStoreFactory.newStore();
    if (configuration.getBoolean(GiraphJob.USE_OUT_OF_CORE_GRAPH,
        GiraphJob.USE_OUT_OF_CORE_GRAPH_DEFAULT)) {
      partitionStore = new DiskBackedPartitionStore<I, V, E, M>(configuration);
    } else {
      partitionStore = new SimplePartitionStore<I, V, E, M>(configuration);
    }

    LOG.debug("starting initializing SASL server..");
    SaslNettyServer.init(configuration);
    LOG.debug("done initializing SASL server.");

    LOG.debug("initializing secret manager..");
    String localJobTokenFile = System.getenv().get(
      UserGroupInformation.HADOOP_TOKEN_FILE_LOCATION);
    if (localJobTokenFile != null) {
      JobConf jobConf = new JobConf(configuration);
      try {
        LOG.debug("loading credentials from localJobTokenFile: " +
          localJobTokenFile);
        Credentials credentials =
          TokenCache.loadTokens(localJobTokenFile, jobConf);
        LOG.debug("credentials loaded are: " + credentials);
        LOG.debug("credentials size: " + credentials.numberOfTokens());
        Collection<Token<? extends TokenIdentifier>> collection =
          credentials.getAllTokens();
        for(Token<? extends TokenIdentifier> token:  collection) {
          TokenIdentifier tokenIdentifier = token.decodeIdentifier();
          LOG.debug("token loaded:" + token);
          LOG.debug("tokenIdentifier:" + tokenIdentifier);
          if (tokenIdentifier instanceof JobTokenIdentifier) {
            Token<JobTokenIdentifier> theToken =
              (Token<JobTokenIdentifier>)token;
            JobTokenIdentifier jobTokenIdentifier =
              (JobTokenIdentifier)tokenIdentifier;
            LOG.debug("cast to job token identifier: " + jobTokenIdentifier);
            secretManager.addTokenForJob(
              jobTokenIdentifier.getJobId().toString(), theToken);
            LOG.debug("JobID: " + jobTokenIdentifier.getJobId());
            LOG.debug("Password for jobTokenIdentifier:" +
              secretManager.retrievePassword(jobTokenIdentifier));
          } else {
            LOG.debug("ignoring non-JobTokenIdentifier: " + tokenIdentifier);
          }
        }
        LOG.debug("loaded credentials.");
      } catch (IOException e) {
        LOG.error("failed to load tokens:" + e);
      }
      LOG.debug("done initializing secret manager.");
    }
  }

  /**
   * Return the partition store for this worker.
   *
   * @return The partition store
   */
  public PartitionStore<I, V, E, M> getPartitionStore() {
    return partitionStore;
  }

  /**
   * Get message store for incoming messages (messages which will be consumed
   * in the next super step)
   *
   * @return Incoming message store
   */
  public MessageStoreByPartition<I, M> getIncomingMessageStore() {
    return incomingMessageStore;
  }

  /**
   * Get message store for current messages (messages which we received in
   * previous super step and which will be consumed in current super step)
   *
   * @return Current message store
   */
  public MessageStoreByPartition<I, M> getCurrentMessageStore() {
    return currentMessageStore;
  }

  /** Prepare for next super step */
  public void prepareSuperstep() {
    if (currentMessageStore != null) {
      try {
        currentMessageStore.clearAll();
      } catch (IOException e) {
        throw new IllegalStateException(
            "Failed to clear previous message store");
      }
    }
    currentMessageStore = incomingMessageStore;
    incomingMessageStore = messageStoreFactory.newStore();
  }

  /**
   * Get the vertex mutations (synchronize on the values)
   *
   * @return Vertex mutations
   */
  public ConcurrentHashMap<I, VertexMutations<I, V, E, M>>
  getVertexMutations() {
    return vertexMutations;
  }
}
