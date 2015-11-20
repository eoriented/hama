/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hama.bsp.sync;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hama.Constants;
import org.apache.hama.bsp.BSPJobID;
import org.apache.hama.bsp.TaskAttemptID;
import org.apache.hama.zookeeper.QuorumPeer;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs.*;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;

import java.net.InetSocketAddress;

public class ZooKeeperRegionBarrierSyncClientImpl extends ZKSyncClient
    implements PeerSyncClient {
  public static final Log LOG = LogFactory.getLog(ZooKeeperRegionBarrierSyncClientImpl.class);

  private static Integer mutex;

  private String quorumServers;
  private ZooKeeper zk;
  private String bspRoot;
  private InetSocketAddress peerAddress;
  private int numBSPTasks;
  // The number of groups for region barrier
  private int numGroups;
  private int numTasksPerGroup;

  private String[] allPeers;

  @Override
  public void init(Configuration conf, BSPJobID jobId, TaskAttemptID taskId) throws Exception {
    quorumServers = QuorumPeer.getZKQuorumServersString(conf);
    this.zk = new ZooKeeper(quorumServers,
        conf.getInt(Constants.ZOOKEEPER_SESSION_TIMEOUT, 1200000), this);
    bspRoot = conf.get(Constants.ZOOKEEPER_ROOT, Constants.DEFAULT_ZOOKEEPER_ROOT);
    String bindAddress = conf.get(Constants.PEER_HOST, Constants.DEFAULT_PEER_HOST);
    int bindPort = conf.getInt(Constants.PEER_PORT, Constants.DEFAULT_PEER_PORT);

    initialize(this.zk, bspRoot);

    peerAddress = new InetSocketAddress(bindAddress, bindPort);
    numBSPTasks = conf.getInt("bsp.peers.num", 1);
    numGroups = conf.getInt("bsp.groups.num", 1);
    numTasksPerGroup = conf.getInt("bsp.tasks.per.group", 1);
  }

  @Override
  public void enterBarrier(BSPJobID jobId, TaskAttemptID taskId, long superstep) throws SyncException {

  }

  @Override
  public void leaveBarrier(BSPJobID jobId, TaskAttemptID taskId, long superstep) throws SyncException {

  }

  @Override
  public void register(BSPJobID jobId, TaskAttemptID taskId, String hostAddress, long port) {
    int retry_count = 0;
    String jobRegisterKey = constructKey(jobId, "groups");
    LOG.info("Root Register Key: " + jobRegisterKey);
    Stat stat = null;

    while (stat != null) {
      try {
        stat = zk.exists(jobRegisterKey, false);
        zk.create(jobRegisterKey, new byte[0], Ids.OPEN_ACL_UNSAFE,
            CreateMode.PERSISTENT);
        Thread.sleep(1000);
      } catch (InterruptedException e) {
        LOG.error(e);
      } catch (KeeperException e) {
        LOG.error(e);
      }

      retry_count++;
      if (retry_count> 0) {
        throw new RuntimeException("Cannot create root node.");
      }
    }

    // Register
    registerGroup(jobId, hostAddress, port, taskId);
  }

  public void registerGroup(BSPJobID jobId, String hostAddress,
                            long port, TaskAttemptID taskId) {
    int taskCount = 0;
    String groupRegisterKey;
    for (int i = 0; i < numGroups; i++) {
      for (int j = 0; j < numTasksPerGroup; j++) {
        groupRegisterKey = constructKey(jobId, "groups", hostAddress + ":" + port);
        LOG.info("Task Register Key: " + groupRegisterKey);
        LOG.info("TaskId: " + taskId + " TaskId.getId(): " + taskId.getTaskID().getId());
        writeNode(groupRegisterKey, taskId, false, null);
      }
    }
  }

  @Override
  public String[] getAllPeerNames(BSPJobID jobID) {
    return new String[0];
  }

  @Override
  public void deregisterFromBarrier(BSPJobID jobId, TaskAttemptID taskId, String hostAddress, long port) {

  }

  @Override
  public void stopServer() {

  }
}
