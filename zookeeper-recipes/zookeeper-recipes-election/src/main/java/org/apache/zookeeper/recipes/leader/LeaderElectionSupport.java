/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.zookeeper.recipes.leader;

import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 * <p>
 * A leader election support library implementing the ZooKeeper election recipe.
 * </p>
 * <p>
 * This support library is meant to simplify the construction of an exclusive
 * leader system on top of Apache ZooKeeper. Any application that can become the
 * leader (usually a process that provides a service, exclusively) would
 * configure an instance of this class with their hostname, at least one
 * listener (an implementation of {@link LeaderElectionAware}), and either an
 * instance of {@link ZooKeeper} or the proper connection information. Once
 * configured, invoking {@link #start()} will cause the client to connect to
 * ZooKeeper and create a leader offer. The library then determines if it has
 * been elected the leader using the algorithm described below. The client
 * application can follow all state transitions via the listener callback.
 * </p>
 * <p>
 * Leader election algorithm
 * </p>
 * <p>
 * The library starts in a START state. Through each state transition, a state
 * start and a state complete event are sent to all listeners. When
 * {@link #start()} is called, a leader offer is created in ZooKeeper. A leader
 * offer is an ephemeral sequential node that indicates a process that can act
 * as a leader for this service. A read of all leader offers is then performed.
 * The offer with the lowest sequence number is said to be the leader. The
 * process elected leader will transition to the leader state. All other
 * processes will transition to a ready state. Internally, the library creates a
 * ZooKeeper watch on the leader offer with the sequence ID of N - 1 (where N is
 * the process's sequence ID). If that offer disappears due to a process
 * failure, the watching process will run through the election determination
 * process again to see if it should become the leader. Note that sequence ID
 * may not be contiguous due to failed processes. A process may revoke its offer
 * to be the leader at any time by calling {@link #stop()}.
 * </p>
 * <p>
 * Guarantees (not) Made and Caveats
 * </p>
 * <p>
 * <ul>
 * <li>It is possible for a (poorly implemented) process to create a leader
 * offer, get the lowest sequence ID, but have something terrible occur where it
 * maintains its connection to ZK (and thus its ephemeral leader offer node) but
 * doesn't actually provide the service in question. It is up to the user to
 * ensure any failure to become the leader - and whatever that means in the
 * context of the user's application - results in a revocation of its leader
 * offer (i.e. that {@link #stop()} is called).</li>
 * <li>It is possible for ZK timeouts and retries to play a role in service
 * liveliness. In other words, if process A has the lowest sequence ID but
 * requires a few attempts to read the other leader offers' sequence IDs,
 * election can seem slow. Users should apply timeouts during the determination
 * process if they need to hit a specific SLA.</li>
 * <li>The library makes a "best effort" to detect catastrophic failures of the
 * process. It is possible that an unforeseen event results in (for instance) an
 * unchecked exception that propagates passed normal error handling code. This
 * normally doesn't matter as the same exception would almost certain destroy
 * the entire process and thus the connection to ZK and the leader offer
 * resulting in another round of leader determination.
 *
 * leader的支持库类，简化leader系统的构建
 * 配置完成之后，client连接zk，会创建一个leader
 * 成为leader的应用，使用主机名配置该类的实例，实例至少有一个listeners
 * client 通过listeners 监听回调，跟踪状态zk的状态改变
 */
public class LeaderElectionSupport implements Watcher {

  private static final Logger logger = LoggerFactory
          .getLogger(LeaderElectionSupport.class);

  private ZooKeeper zooKeeper;

  private State state;
  private Set<LeaderElectionAware> listeners;

  private String rootNodeName;
  private LeaderOffer leaderOffer;
  private String hostName;

  /**
   * 初始化操作：
   * 1、初始化状态 STOP
   * 2、创建线程安全的 LeaderElectionAware 集合
   *    LeaderElectionAware 的作用用于监听服务状态改变
   */
  public LeaderElectionSupport() {
    state = State.STOP;
    listeners = Collections.synchronizedSet(new HashSet<LeaderElectionAware>());
  }

  /**
   * 启动选举过程，此方法将创建一个 leader offer
   * 当创建leader时，任何故障，都会发送失败异常到 listeners 的监听
   * 创建leader 步骤：
   * 1.改变State 状态为start
   * 2.将 start 状态通知给所有实现 LeaderElectionAware接口的节点，告诉大家开始选举leader了
   * 3. 开始创建 leader offer 直到 leader offer 的创建完成
   * 4. 确认当前已选择的leader 服务的状态
   */
  public synchronized void start() {
    //1.改变State状态为start
    state = State.START;
    //2.将 start 状态通知给所有实现 LeaderElectionAware接口的节点，告诉大家开始选举leader了
    dispatchEvent(EventType.START);

    logger.info("Starting leader election support");

    if (zooKeeper == null) {
      throw new IllegalStateException(
              "No instance of zookeeper provided. Hint: use setZooKeeper()");
    }

    if (hostName == null) {
      throw new IllegalStateException(
              "No hostname provided. Hint: use setHostName()");
    }

    try {
      //3. 开始创建 leader offer 直到 leader offer 的创建完成
      makeOffer();
      //4. 确认当前已选择的leader 服务的状态
      determineElectionStatus();
    } catch (KeeperException e) {
      // 抛出leader的创建异常
      becomeFailed(e);
      return;
    } catch (InterruptedException e) {
      becomeFailed(e);
      return;
    }
  }

  /**
   * 停止所有的选举服务，撤销leader offer 的提名，断开zookeeper连接
   */
  public synchronized void stop() {
    state = State.STOP;
    dispatchEvent(EventType.STOP_START);

    logger.info("Stopping leader election support");

    if (leaderOffer != null) {
      try {
        zooKeeper.delete(leaderOffer.getNodePath(), -1);
        logger.info("Removed leader offer {}", leaderOffer.getNodePath());
      } catch (InterruptedException e) {
        becomeFailed(e);
      } catch (KeeperException e) {
        becomeFailed(e);
      }
    }

    dispatchEvent(EventType.STOP_COMPLETE);
  }

  /**
   * 开始创建 leader offer 直到 leader offer 的创建完成
   * @throws KeeperException
   * @throws InterruptedException
   */
  private void makeOffer() throws KeeperException, InterruptedException {
    //1. 改变State状态为 OFFER
    state = State.OFFER;
    //2. 通知所有节点状态改变为 OFFER_START
    dispatchEvent(EventType.OFFER_START);
    //3. 创建 LeaderOffer 对象
    LeaderOffer newLeaderOffer = new LeaderOffer();
    byte[] hostnameBytes;
    //4. 加锁写入 LeaderOffer 属性：hostName、NodePath
    synchronized (this) {
      newLeaderOffer.setHostName(hostName);
      hostnameBytes = hostName.getBytes();
      newLeaderOffer.setNodePath(zooKeeper.create(rootNodeName + "/" + "n_",
              hostnameBytes, ZooDefs.Ids.OPEN_ACL_UNSAFE,
              CreateMode.EPHEMERAL_SEQUENTIAL));
      leaderOffer = newLeaderOffer;
    }
    logger.debug("Created leader offer {}", leaderOffer);
    //5. 通知所有节点，LeaderOffer创建完成
    dispatchEvent(EventType.OFFER_COMPLETE);
  }

  private synchronized LeaderOffer getLeaderOffer() {
    return leaderOffer;
  }

  /**
   * 再次 确认当前 leader 是当前zk服务的 leader 节点
   * @throws KeeperException
   * @throws InterruptedException
   */
  private void determineElectionStatus() throws KeeperException,
          InterruptedException {
    //1. 标记State.DETERMINE 待确定
    state = State.DETERMINE;
    //2. DETERMINE_START 状态发送给所有节点
    dispatchEvent(EventType.DETERMINE_START);
    //3. 获取当前的 LeaderOffer 对象
    LeaderOffer currentLeaderOffer = getLeaderOffer();
    //4. 从当前 LeaderOffer 对象中获取NodePath 并用“/”切割
    String[] components = currentLeaderOffer.getNodePath().split("/");
    //5. 给当前 LeaderOffer 设置id
    currentLeaderOffer.setId(Integer.valueOf(components[components.length - 1]
            .substring("n_".length())));
    //6. 给当前的rootNodeName 下的所有znode 发送 leaderOffer
    List<LeaderOffer> leaderOffers = toLeaderOffers(zooKeeper.getChildren(
            rootNodeName, false));

    /*
     * 7. 确认最终的leader 和 ready
     * a.拿到排序的 leaderOffers ，判断 leaderOffer 的id和当前 leaderOffer id进行对比，
     * 第一个将成为leader
     * b. 其他的将成为 Ready
     */
    for (int i = 0; i < leaderOffers.size(); i++) {
      LeaderOffer leaderOffer = leaderOffers.get(i);

      if (leaderOffer.getId().equals(currentLeaderOffer.getId())) {
        logger.debug("There are {} leader offers. I am {} in line.",
                leaderOffers.size(), i);

        dispatchEvent(EventType.DETERMINE_COMPLETE);

        if (i == 0) {
          becomeLeader();
        } else {
          becomeReady(leaderOffers.get(i - 1));
        }

        //遍历之后，一旦确认leader的位置，就完成
        break;
      }
    }
  }

  /**
   * 没有被选成Leader 的节点变为READY 状态
   * @param neighborLeaderOffer
   * @throws KeeperException
   * @throws InterruptedException
   */
  private void becomeReady(LeaderOffer neighborLeaderOffer)
          throws KeeperException, InterruptedException {

    logger.info("{} not elected leader. Watching node:{}",
            getLeaderOffer().getNodePath(), neighborLeaderOffer.getNodePath());

    /*
     * 确保显式的Watcher，用于共享改zk实例
     */
    Stat stat = zooKeeper.exists(neighborLeaderOffer.getNodePath(), this);

    if (stat != null) {
      dispatchEvent(EventType.READY_START);
      logger.debug(
              "We're behind {} in line and they're alive. Keeping an eye on them.",
              neighborLeaderOffer.getNodePath());
      state = State.READY;
      dispatchEvent(EventType.READY_COMPLETE);
    } else {
      /*
       * If the stat fails, the node has gone missing between the call to
       * getChildren() and exists(). We need to try and become the leader.
       */
      logger
              .info(
                      "We were behind {} but it looks like they died. Back to determination.",
                      neighborLeaderOffer.getNodePath());
      determineElectionStatus();
    }

  }

  /**
   * 确认之后，最终成为的Leader ELECTED_COMPLETE通知到其他节点
   */
  private void becomeLeader() {
    state = State.ELECTED;
    dispatchEvent(EventType.ELECTED_START);

    logger.info("Becoming leader with node:{}", getLeaderOffer().getNodePath());

    dispatchEvent(EventType.ELECTED_COMPLETE);
  }

  /**
   * 最终失败的调用
   * @param e
   */
  private void becomeFailed(Exception e) {
    logger.error("Failed in state {} - Exception:{}", state, e);

    state = State.FAILED;
    dispatchEvent(EventType.FAILED);
  }

  /**
   * @return hostname of the current leader
   * @throws KeeperException
   * @throws InterruptedException
   */
  public String getLeaderHostName() throws KeeperException,
          InterruptedException {

    List<LeaderOffer> leaderOffers = toLeaderOffers(zooKeeper.getChildren(
            rootNodeName, false));

    if (leaderOffers.size() > 0) {
      return leaderOffers.get(0).getHostName();
    }

    return null;
  }

  /**
   * 将发给 LeaderOffers 的根节点一下的所有节点都变成一个leader offer（每一个Node都创建LeaderOffer）
   * @param strings zk 的 rootNodeName 中获取到的子节点集合
   * @return
   * @throws KeeperException
   * @throws InterruptedException
   */
  private List<LeaderOffer> toLeaderOffers(List<String> strings)
          throws KeeperException, InterruptedException {

    List<LeaderOffer> leaderOffers = new ArrayList<LeaderOffer>(strings.size());

    /**
     * 遍历将每个rootNodeName的孩子变成一个leader offer
     */
    for (String offer : strings) {
      String hostName = new String(zooKeeper.getData(
              rootNodeName + "/" + offer, false, null));

      leaderOffers.add(new LeaderOffer(Integer.valueOf(offer.substring("n_"
              .length())), rootNodeName + "/" + offer, hostName));
    }

    /*
     * 按照序列号进行排序，目的方便设置 watches
     */
    Collections.sort(leaderOffers, new LeaderOffer.IdComparator());

    return leaderOffers;
  }

  // TODO: 2019/8/20  学习 Watch 之后，在回头来回顾该方法的作用
  @Override
  public void process(WatchedEvent event) {
    if (event.getType().equals(Watcher.Event.EventType.NodeDeleted)) {
      if (!event.getPath().equals(getLeaderOffer().getNodePath())
              && state != State.STOP) {
        logger.debug(
                "Node {} deleted. Need to run through the election process.",
                event.getPath());
        try {
          determineElectionStatus();
        } catch (KeeperException e) {
          becomeFailed(e);
        } catch (InterruptedException e) {
          becomeFailed(e);
        }
      }
    }
  }

  /**
   * 通知事件类型的方法
   * 通过实现 LeaderElectionAware 接口 获取 eventType的改变
   * @param eventType
   */
  private void dispatchEvent(EventType eventType) {
    logger.debug("Dispatching event:{}", eventType);

    synchronized (listeners) {
      if (listeners.size() > 0) {
        for (LeaderElectionAware observer : listeners) {
          observer.onElectionEvent(eventType);
        }
      }
    }
  }

  /**
   * Adds {@code listener} to the list of listeners who will receive events.
   *
   * @param listener
   */
  public void addListener(LeaderElectionAware listener) {
    listeners.add(listener);
  }

  /**
   * Remove {@code listener} from the list of listeners who receive events.
   *
   * @param listener
   */
  public void removeListener(LeaderElectionAware listener) {
    listeners.remove(listener);
  }

  @Override
  public String toString() {
    return "{ state:" + state + " leaderOffer:" + getLeaderOffer() + " zooKeeper:"
            + zooKeeper + " hostName:" + getHostName() + " listeners:" + listeners
            + " }";
  }

  /**
   * <p>
   * Gets the ZooKeeper root node to use for this service.
   * </p>
   * <p>
   * For instance, a root node of {@code /mycompany/myservice} would be the
   * parent of all leader offers for this service. Obviously all processes that
   * wish to contend for leader status need to use the same root node. Note: We
   * assume this node already exists.
   * </p>
   *
   * @return a znode path
   */
  public String getRootNodeName() {
    return rootNodeName;
  }

  /**
   * <p>
   * Sets the ZooKeeper root node to use for this service.
   * </p>
   * <p>
   * For instance, a root node of {@code /mycompany/myservice} would be the
   * parent of all leader offers for this service. Obviously all processes that
   * wish to contend for leader status need to use the same root node. Note: We
   * assume this node already exists.
   * </p>
   */
  public void setRootNodeName(String rootNodeName) {
    this.rootNodeName = rootNodeName;
  }

  /**
   * The {@link ZooKeeper} instance to use for all operations. Provided this
   * overrides any connectString or sessionTimeout set.
   */
  public ZooKeeper getZooKeeper() {
    return zooKeeper;
  }

  public void setZooKeeper(ZooKeeper zooKeeper) {
    this.zooKeeper = zooKeeper;
  }

  /**
   * The hostname of this process. Mostly used as a convenience for logging and
   * to respond to {@link #getLeaderHostName()} requests.
   */
  public synchronized String getHostName() {
    return hostName;
  }

  public synchronized void setHostName(String hostName) {
    this.hostName = hostName;
  }

  /**
   * // TODO: 2019/8/20  
   * The type of event.
   */
  public static enum EventType {
    START, OFFER_START, OFFER_COMPLETE, DETERMINE_START, DETERMINE_COMPLETE, ELECTED_START, ELECTED_COMPLETE, READY_START, READY_COMPLETE, FAILED, STOP_START, STOP_COMPLETE,
  }

  /**
   * The internal state of the election support service.
   */
  public static enum State {
    START, OFFER, DETERMINE, ELECTED, READY, FAILED, STOP
  }
}
