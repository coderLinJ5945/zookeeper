/**
 *
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
package org.apache.zookeeper.recipes.lock;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.SortedSet;
import java.util.TreeSet;

import static org.apache.zookeeper.CreateMode.EPHEMERAL_SEQUENTIAL;

/**
 * A <a href="package.html">protocol to implement an exclusive
 *  write lock or to elect a leader</a>. <p/> You invoke {@link #lock()} to 
 *  start the process of grabbing the lock; you may get the lock then or it may be 
 *  some time later. <p/> You can register a listener so that you are invoked 
 *  when you get the lock; otherwise you can ask if you have the lock
 *  by calling {@link #isOwner()}
 *  写入锁（独占锁）,zk锁的核心类
 */
public class WriteLock extends ProtocolSupport {
    private static final Logger LOG = LoggerFactory.getLogger(WriteLock.class);
    //要上锁节点的父路径
    private final String dir;
    private String id;
    private ZNodeName idName;
    private String ownerId;
    private String lastChildId;
    private byte[] data = {0x12, 0x34};

    /** 锁监听回调接口*/
    private LockListener callback;

    /** 内部类， 用于 zk 锁的操作（简单理解锁的赋能） */
    private LockZooKeeperOperation zop;
    
    /**
     * zookeeper contructor for writelock
     * writelock 的构造器
     * @param zookeeper zookeeper client 实例
     * @param dir 要上锁节点的父路径
    // TODO: 2019/8/22  ACL 待理解
     * @param acl the acls that you want to use for all the paths, 
     * if null world read/write is used.
     */
    public WriteLock(ZooKeeper zookeeper, String dir, List<ACL> acl) {
        super(zookeeper);
        this.dir = dir;
        if (acl != null) {
            setAcl(acl);
        }
        this.zop = new LockZooKeeperOperation();
    }
    
    /**
     * zookeeper contructor for writelock with callback
     * 带回调的构造器，用于返回锁触发事件进行后续处理
     * @param zookeeper the zookeeper client instance
     * @param dir the parent path you want to use for locking
     * @param acl the acls that you want to use for all the paths
     * @param callback the call back instance
     */
    public WriteLock(ZooKeeper zookeeper, String dir, List<ACL> acl, 
            LockListener callback) {
        this(zookeeper, dir, acl);
        this.callback = callback;
    }

    /**
     * 返回当前的锁监听对象
     * @return the locklistener
     */
    public synchronized LockListener getLockListener() {
        return this.callback;
    }
    
    /**
     * register a different call back listener
     * @param callback the call back instance
     */
    public synchronized void setLockListener(LockListener callback) {
        this.callback = callback;
    }

    /**
     * Removes the lock or associated znode if 
     * you no longer require the lock. this also 
     * removes your request in the queue for locking
     * in case you do not already hold the lock.
     * @throws RuntimeException throws a runtime exception
     * if it cannot connect to zookeeper.
     * 移除 lock 操作
     * 1. 删除 zookeeper 上的 zopde
     * 2. 置空 id 并通知回调
     */
    public synchronized void unlock() throws RuntimeException {
        /*
         *  判断锁是否是关闭状态和锁id
         *  如果 锁不为关闭状态并且不为空，才执行后续关闭锁操作
         */
        if (!isClosed() && id != null) {
            /**
             * 如果 不能重新连接到ZK，作者不想在关闭时挂起这个进程
             * 在失败的情况下，我们不需要重试这个操作，因为ZK服务端将删除临时文件
             */
            try {
                //定义 zk的option 操作
                ZooKeeperOperation zopdel = new ZooKeeperOperation() {
                    public boolean execute() throws KeeperException,
                        InterruptedException {
                        //删除具有给定路径的节点 -1：表示与任意版本匹配
                        zookeeper.delete(id, -1);   
                        return Boolean.TRUE;
                    }
                };
                /**
                 *  执行zk的option调用，这里可以用于借鉴
                 */
                zopdel.execute();
            } catch (InterruptedException e) {
                LOG.warn("Caught: " + e, e);
                //set that we have been interrupted.
               Thread.currentThread().interrupt();
            } catch (KeeperException.NoNodeException e) {
                // do nothing
            } catch (KeeperException e) {
                LOG.warn("Caught: " + e, e);
                throw (RuntimeException) new RuntimeException(e.getMessage()).
                    initCause(e);
            }
            finally {
                /* 最后获取监听，释放锁*/
                // 监听的作用是，用于回调，将锁释放后回调给后续实现类
                LockListener lockListener = getLockListener();
                if (lockListener != null) {
                    lockListener.lockReleased();
                }
                // 简单理解释放锁的操作，就是把id 置空
                id = null;
            }
        }
    }
    
    /**
     * 观察者模式，观察上锁事件是否出现异常
     */
    private class LockWatcher implements Watcher {
        public void process(WatchedEvent event) {
            // lets either become the leader or watch the new/updated node
            LOG.debug("Watcher fired on path: " + event.getPath() + " state: " + 
                    event.getState() + " type " + event.getType());
            try {
                lock();
            } catch (Exception e) {
                LOG.warn("Failed to acquire lock: " + e, e);
            }
        }
    }
    
    /**
     * a zoookeeper operation that is mainly responsible
     * for all the magic required for locking.
     */
    private  class LockZooKeeperOperation implements ZooKeeperOperation {
        
        /** find if we have been created earler if not create our node
         * 
         * @param prefix the prefix node
         * @param zookeeper teh zookeeper client
         * @param dir the dir paretn
         * @throws KeeperException
         * @throws InterruptedException
         */
        private void findPrefixInChildren(String prefix, ZooKeeper zookeeper, String dir) 
            throws KeeperException, InterruptedException {
            List<String> names = zookeeper.getChildren(dir, false);
            for (String name : names) {
                if (name.startsWith(prefix)) {
                    id = name;
                    if (LOG.isDebugEnabled()) {
                        LOG.debug("Found id created last time: " + id);
                    }
                    break;
                }
            }
            if (id == null) {
                id = zookeeper.create(dir + "/" + prefix, data, 
                        getAcl(), EPHEMERAL_SEQUENTIAL);

                if (LOG.isDebugEnabled()) {
                    LOG.debug("Created id: " + id);
                }
            }

        }
        
        /**
         * 真正重试获取锁的操作方法
         * // TODO: 2019/8/22 待细读获取独占锁的逻辑 
         */
        @SuppressFBWarnings(value = "NP_NULL_PARAM_DEREF_NONVIRTUAL",
                justification = "findPrefixInChildren will assign a value to this.id")
        public boolean execute() throws KeeperException, InterruptedException {
            // do while 重复尝试，直到获取到锁id
            do {
                if (id == null) {
                    long sessionId = zookeeper.getSessionId();
                    String prefix = "x-" + sessionId + "-";
                    // lets try look up the current ID if we failed 
                    // in the middle of creating the znode
                    findPrefixInChildren(prefix, zookeeper, dir);
                    idName = new ZNodeName(id);
                }
                List<String> names = zookeeper.getChildren(dir, false);
                if (names.isEmpty()) {
                    LOG.warn("No children in: " + dir + " when we've just " +
                    "created one! Lets recreate it...");
                    // lets force the recreation of the id
                    id = null;
                } else {
                    // lets sort them explicitly (though they do seem to come back in order ususally :)
                    SortedSet<ZNodeName> sortedNames = new TreeSet<ZNodeName>();
                    for (String name : names) {
                        sortedNames.add(new ZNodeName(dir + "/" + name));
                    }
                    ownerId = sortedNames.first().getName();
                    SortedSet<ZNodeName> lessThanMe = sortedNames.headSet(idName);
                    if (!lessThanMe.isEmpty()) {
                        ZNodeName lastChildName = lessThanMe.last();
                        lastChildId = lastChildName.getName();
                        if (LOG.isDebugEnabled()) {
                            LOG.debug("watching less than me node: " + lastChildId);
                        }
                        Stat stat = zookeeper.exists(lastChildId, new LockWatcher());
                        if (stat != null) {
                            return Boolean.FALSE;
                        } else {
                            LOG.warn("Could not find the" +
                                            " stats for less than me: " + lastChildName.getName());
                        }
                    } else {
                        if (isOwner()) {
                            LockListener lockListener = getLockListener();
                            if (lockListener != null) {
                                lockListener.lockAcquired();
                            }
                            return Boolean.TRUE;
                        }
                    }
                }
            }
            while (id == null);
            return Boolean.FALSE;
        }
    };

    /**
     * 尝试获取独占锁
     */
    public synchronized boolean lock() throws KeeperException, InterruptedException {
        //锁为关闭状态，直接返回fasle
        if (isClosed()) {
            return false;
        }
        //确保给定路径不存在任何数据、当前ACL和任何标志
        ensurePathExists(dir);
        // 尝试重试获取锁，最终是调用了LockZooKeeperOperation.execute()方法进行尝试获取
        return (Boolean) retryOperation(zop);
    }

    /**
     * 返回 lock 的 父级目录
     * @return the parent dir used for locks.
     */
    public String getDir() {
        return dir;
    }

    /**
     * 如果 该 node 是 lock的拥有者或者是leader 返回true
     */
    public boolean isOwner() {
        return id != null && ownerId != null && id.equals(ownerId);
    }

    /**
     * 返回lock 的id
     * @return the id for this lock
     */
    public String getId() {
       return this.id;
    }
}

