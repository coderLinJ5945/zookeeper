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

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * A base class for protocol implementations which provides a number of higher 
 * level helper methods for working with ZooKeeper along with retrying synchronous
 *  operations if the connection to ZooKeeper closes such as 
 *  {@link #retryOperation(ZooKeeperOperation)}
 *  Protocol 的支持类 ：用于zk连接、关闭时，于zk一起工作，并尝试同步操作
 *  todo 还不理解 Protocol具体作用，只能初略分析功能
 */
class ProtocolSupport {
    private static final Logger LOG = LoggerFactory.getLogger(ProtocolSupport.class);

    protected final ZooKeeper zookeeper;
    /** AtomicBoolean 并发包中的原子更新标志，这里用于确定锁的开关状态 */
    private AtomicBoolean closed = new AtomicBoolean(false);
    //重连延迟
    private long retryDelay = 500L;
    //重连次数
    private int retryCount = 10;

    // TODO: 2019/8/22  ACL Access Control List 待深入理解
    private List<ACL> acl = ZooDefs.Ids.OPEN_ACL_UNSAFE;

    public ProtocolSupport(ZooKeeper zookeeper) {
        this.zookeeper = zookeeper;
    }

    /**
     *  关闭此策略，只是释放 zk resources 但是保持 zk的实例打开
     */
    public void close() {
        if (closed.compareAndSet(false, true)) {
            doClose();
        }
    }
    
    /**
     * 获取 zookeeper client 实例
     * @return zookeeper client instance
     */
    public ZooKeeper getZookeeper() {
        return zookeeper;
    }

    /**
     * return the acl its using
     * @return the acl.
     */
    public List<ACL> getAcl() {
        return acl;
    }

    /**
     * set the acl 
     * @param acl the acl to set to
     */
    public void setAcl(List<ACL> acl) {
        this.acl = acl;
    }


    public long getRetryDelay() {
        return retryDelay;
    }


    public void setRetryDelay(long retryDelay) {
        this.retryDelay = retryDelay;
    }

    /**
     * Allow derived classes to perform 
     * some custom closing operations to release resources
     */
    protected void doClose() {
    }


    /**
     * 执行给定的ZooKeeperOperation 操作，如果连接失败，则重试
     * @return object. it needs to be cast to the callee's expected 
     * return type.
     */
    protected Object retryOperation(ZooKeeperOperation operation) 
        throws KeeperException, InterruptedException {
        KeeperException exception = null;
        for (int i = 0; i < retryCount; i++) {
            try {
                return operation.execute();
            } catch (KeeperException.SessionExpiredException e) {
                LOG.warn("Session expired for: " + zookeeper + " so reconnecting due to: " + e, e);
                throw e;
            } catch (KeeperException.ConnectionLossException e) {
                if (exception == null) {
                    exception = e;
                }
                LOG.debug("Attempt " + i + " failed with connection loss so " +
                		"attempting to reconnect: " + e, e);
                retryDelay(i);
            }
        }
        throw exception;
    }

    /**
     * 确保给定路径不存在任何数据、当前ACL和任何标志
     * @param path
     */
    protected void ensurePathExists(String path) {
        ensureExists(path, null, acl, CreateMode.PERSISTENT);
    }

    /**
     * 确保给定的路径与给定的数据、ACL和标志一起存在
     * @param path
     * @param acl
     * @param flags
     */
    protected void ensureExists(final String path, final byte[] data,
            final List<ACL> acl, final CreateMode flags) {
        try {
            retryOperation(new ZooKeeperOperation() {
                public boolean execute() throws KeeperException, InterruptedException {
                    Stat stat = zookeeper.exists(path, false);
                    if (stat != null) {
                        return true;
                    }
                    zookeeper.create(path, data, acl, flags);
                    return true;
                }
            });
        } catch (KeeperException e) {
            LOG.warn("Caught: " + e, e);
        } catch (InterruptedException e) {
            LOG.warn("Caught: " + e, e);
        }
    }

    /**
     * 返回 protocol 是否是closed状态
     * @return true if this protocol is closed
     */
    protected boolean isClosed() {
        return closed.get();
    }

    /**
     * Performs a retry delay if this is not the first attempt
     * @param attemptCount the number of the attempts performed so far
     */
    protected void retryDelay(int attemptCount) {
        if (attemptCount > 0) {
            try {
                Thread.sleep(attemptCount * retryDelay);
            } catch (InterruptedException e) {
                LOG.debug("Failed to sleep: " + e, e);
            }
        }
    }
}
