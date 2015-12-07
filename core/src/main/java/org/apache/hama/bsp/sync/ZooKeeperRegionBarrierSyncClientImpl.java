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
import org.apache.zookeeper.*;
import org.apache.zookeeper.ZooDefs.*;
import org.apache.zookeeper.data.Stat;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Collections;
import java.util.List;

public class ZooKeeperRegionBarrierSyncClientImpl extends ZKSyncClient
    implements PeerSyncClient {
  public static final Log LOG = LogFactory.getLog(ZooKeeperRegionBarrierSyncClientImpl.class);

  private volatile Integer mutex = 0;

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
    //numBSPTasks = conf.getInt("bsp.peers.num", 1);
    numGroups = conf.getInt("bsp.groups.num", 1);
    numTasksPerGroup = conf.getInt("bsp.tasks.per.group", 1);
    LOG.info("Start connecting to Zookeeper! At " + peerAddress);
  }

  @Override
  public void enterBarrier(BSPJobID jobId, TaskAttemptID taskId, long superstep) throws SyncException {
    LOG.info("[" + getPeerName() + "] enter the barrier: " + superstep);
    String groupId;
    try {
      synchronized (zk) {
        groupId = getGroupId(taskId);

        final String pathToSuperstepZnodePerGroup = constructKey(taskId.getJobID(),
            "groups", groupId, "sync", Long.toString(superstep));

        //LOG.info("path to superstep znode  : " + pathToSuperstepZnodePerGroup);

        writeNode(pathToSuperstepZnodePerGroup, null, true, null);

        BarrierWatcher barrierWatcher = new BarrierWatcher();
        zk.exists(pathToSuperstepZnodePerGroup + "/ready", barrierWatcher);
        zk.create(constructKey(taskId.getJobID(), "groups", groupId, "sync",
                Long.toString(superstep), taskId.toString()), null, Ids.OPEN_ACL_UNSAFE,
            CreateMode.EPHEMERAL);

        List<String> znodes = zk.getChildren(pathToSuperstepZnodePerGroup, false);
        //LOG.info("enterbarrier childnode: " + znodes.toString());

        int size = znodes.size();
        boolean hasReady = znodes.contains("ready");
        if (hasReady) {
          size--;
        }

        LOG.debug("===> at superstep :" + superstep + " current znode size: "
            + znodes.size() + " current znodes:" + znodes);

        LOG.debug("enterBarrier() znode size within " + pathToSuperstepZnodePerGroup
            + " is " + znodes.size() + ". Znodes include " + znodes);

        if (size < numTasksPerGroup) {
          while (!barrierWatcher.isComplete()) {
            if (!hasReady) {
              synchronized (mutex) {
                mutex.wait(1000);
              }
            }
          }
          LOG.info("2. at superstep: " + superstep + " after waiting ..."
              + taskId.toString());
        } else {
          LOG.info("---> at superstep: " + superstep
              + " task that is creating /ready znode:" + taskId.toString());
          writeNode(pathToSuperstepZnodePerGroup + "/ready", null, false, null);
        }
      }
    } catch (Exception e) {
      LOG.error(e.toString());
      throw new SyncException(e.toString());
    }
  }

  @Override
  public void leaveBarrier(BSPJobID jobId, final TaskAttemptID taskId, final long superstep)
      throws SyncException {
    try {
      // Why is below variable declared to final?
      String groupId = getGroupId(taskId);

      while (true) {
        final String pathToSuperstepZnode = constructKey(taskId.getJobID(),
            "groups", groupId, "sync", Long.toString(superstep));
        List<String> znodes = zk.getChildren(pathToSuperstepZnode, false);
        LOG.info("leaveBarrier() !!! checking znodes contains /ready node or not:" +
            " groupId: " + groupId + " at superstep:" + superstep + " znode:" + znodes);

        if (znodes.contains("ready"))
          znodes.remove("ready");

        final int size = znodes.size();

        LOG.info("leaveBarrier() groups: " + groupId + " at superstep:" + superstep +
            " znode size: (" + size + ") znodes:" + znodes);

        if (null == znodes || znodes.isEmpty())
          return;
        if (1 == size) {
          try {
            zk.delete(getNodeNameinGroup(taskId, superstep, groupId), 0);
          } catch (KeeperException.NoNodeException nne) {
            LOG.debug(
                "+++ (znode size is 1). Ignore because znode may disconnect.",
                nne);
          }
          return;
        }

        Collections.sort(znodes);

        String lowest = znodes.get(0);
        String highest = znodes.get(size - 1);

        synchronized (mutex) {
          if (getNodeNameinGroup(taskId, superstep, groupId).equals(
              pathToSuperstepZnode + "/" + lowest)) {
            Stat s = zk.exists(pathToSuperstepZnode + "/" + highest, new Watcher() {
              @Override
              public void process(WatchedEvent watchedEvent) {
                synchronized (mutex) {
                  LOG.info("leaveBarrier() at superstep: " + superstep
                      + " taskid:" + taskId.toString()
                      + " highest notify lowest.");
                  mutex.notifyAll();
                }
              }
            });

            if (s != null) {
              LOG.info("leaveBarrier(): superstep:" + superstep + " taskid:"
                  + taskId.toString() + " wait for higest notify.");
              mutex.wait();
            }
          } else {

            if (zk.exists(getNodeNameinGroup(taskId, superstep, groupId), false)
                != null) {
              try {
                zk.delete(getNodeNameinGroup(taskId, superstep, groupId), 0);
              } catch (KeeperException.NoNodeException e) {
                LOG.error(e);
              }
            }

            Stat s = zk.exists(pathToSuperstepZnode + "/" + lowest,
                new Watcher() {
                  @Override
                  public void process(WatchedEvent watchedEvent) {
                    synchronized (mutex) {
                      LOG.info("leaveBarrier() at superstep: " + superstep
                          + " taskid:" + taskId.toString()
                          + " lowest notify other nodes.");
                      mutex.notifyAll();
                    }
                  }
                });

            if (s != null) {
                LOG.info("leaveBarrier(): superstep:" + superstep + " taskid:"
                    + taskId.toString() + " wait for lowest notify.");
                mutex.wait();
            }
          }
        }
      }
    } catch (Exception e) {
      throw new SyncException(e.getMessage());
    }
  }

  @Override
  public void register(BSPJobID jobId, TaskAttemptID taskId, String hostAddress, long port) {
    int retry_count = 0;
    String jobRegisterKey = constructKey(jobId, "peers");
    Stat stat = null;

    while (stat != null) {
      try {
        stat = zk.exists(jobRegisterKey, false);
        zk.create(jobRegisterKey, new byte[0], Ids.OPEN_ACL_UNSAFE,
            CreateMode.PERSISTENT);
        Thread.sleep(1000);
      } catch (Exception e) {
        LOG.debug(e);
      }

      retry_count++;
      if (retry_count > 9) {
        throw new RuntimeException("Cannot create root node.");
      }
    }

    // Register groups
    registerGroup(jobId, hostAddress, port, taskId);

//    try {
//      LOG.info("parent of children: "+ constructKey(jobId));
//      List<String> list = zk.getChildren(constructKey(jobId, "groups"), true);
//      LOG.info("/bsp/jobId/groups/: " + list);
//      list = zk.getChildren(constructKey(jobId, "groups",
//          Integer.toString(taskId.getTaskID().getId() % numGroups)), true);
//      LOG.info("/bsp/jobId/groups/" + Integer.toString(taskId.getTaskID().getId())
//          + "/:" + list);
//    } catch (InterruptedException e) {
//      e.printStackTrace();
//    } catch (KeeperException e) {
//      e.printStackTrace();
//    }

  }

  public void registerGroup(BSPJobID jobId, String hostAddress, long port,
                            TaskAttemptID taskId) {
    String groupRegisterKey;
    int selectedGroup;

    // Register the number of groups which is set in configuration, "bsp.groups.num"
    // into zookeeper.
    selectedGroup = taskId.getTaskID().getId() / numTasksPerGroup;
    groupRegisterKey = constructKey(jobId,"peers", "groups",
        Integer.toString(selectedGroup), hostAddress + ":" + port);
    LOG.info("Task Register Key: " + groupRegisterKey + " taskid: " + taskId);
    writeNode(groupRegisterKey, taskId, false, null);
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

  public String getPeerName() {
    return peerAddress.getHostName() + ":" + peerAddress.getPort();
  }

  public String getGroupId(TaskAttemptID taskId) {
    return Integer.toString(taskId.getTaskID().getId() / numTasksPerGroup);
  }

  private class BarrierWatcher implements Watcher {
    private boolean complete = false;

    boolean isComplete() {
      return this.complete;
    }

    @Override
    public void process(WatchedEvent event) {
      this.complete = true;
      synchronized (mutex) {
        mutex.notifyAll();
      }
    }
  }
}