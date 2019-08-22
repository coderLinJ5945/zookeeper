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

package org.apache.zookeeper.server;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.apache.jute.InputArchive;
import org.apache.jute.OutputArchive;
import org.apache.jute.Record;
import org.apache.zookeeper.data.Stat;
import org.apache.zookeeper.data.StatPersisted;

import java.io.IOException;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

/**
 * znode 节点的信息类
 * 数据节点包含对其父节点的引用、作为其数据的字节数组、acl数组、stat对象及其子路径集
 *
 */
@SuppressFBWarnings("EI_EXPOSE_REP2")
public class DataNode implements Record {
    /** datanode 的 data */
    byte data[];

    /**
     * dataNode 的访问控制集合
     */
    Long acl;

    /**
     * 保存到磁盘的此节点Stat
     */
    public StatPersisted stat;

    /**
     * 此节点的子列表。注意，子字符串列表不包含父路径——只包含路径的最后一部分
     */
    private Set<String> children = null;

    private static final Set<String> EMPTY_SET = Collections.emptySet();

    /**
     * 默认构造器
     */
    DataNode() {

    }

    /**
     * znode 数据带参构造器
     * @param data 数据
     *
     * @param acl Access Control List  node 访问控制集合
     *
     * @param stat  Stat 的持久类
     *
     */
    public DataNode(byte data[], Long acl, StatPersisted stat) {
        this.data = data;
        this.acl = acl;
        this.stat = stat;
    }

    /**
     * 添加child node
     * 其实是HashSet 集合进行添加和删除操作
     * @param child
     *            to be inserted
     * @return true if this set did not already contain the specified element
     */
    public synchronized boolean addChild(String child) {
        if (children == null) {
            //初始大小设置为8
            children = new HashSet<String>(8);
        }
        return children.add(child);
    }

    /**
     * 移除Child
     * @param child
     * @return true if this set contained the specified element
     */
    public synchronized boolean removeChild(String child) {
        if (children == null) {
            return false;
        }
        return children.remove(child);
    }

    /**
     * 直接设置该datanode的子节点
     * @param children
     */
    public synchronized void setChildren(HashSet<String> children) {
        this.children = children;
    }

    /**
     * 获取 Children
     * 使用 Collections.unmodifiableSet 返回一个不可修改的Children 集合
     */
    public synchronized Set<String> getChildren() {
        if (children == null) {
            return EMPTY_SET;
        }

        return Collections.unmodifiableSet(children);
    }

    public synchronized long getApproximateDataSize() {
        if(null==data) return 0;
        return data.length;
    }

    /**
     * 复制Stat
     * @param to
     */
    synchronized public void copyStat(Stat to) {
        to.setAversion(stat.getAversion());
        to.setCtime(stat.getCtime());
        to.setCzxid(stat.getCzxid());
        to.setMtime(stat.getMtime());
        to.setMzxid(stat.getMzxid());
        to.setPzxid(stat.getPzxid());
        to.setVersion(stat.getVersion());
        to.setEphemeralOwner(getClientEphemeralOwner(stat));
        to.setDataLength(data == null ? 0 : data.length);
        int numChildren = 0;
        if (this.children != null) {
            numChildren = children.size();
        }
        // when we do the Cversion we need to translate from the count of the creates
        // to the count of the changes (v3 semantics)
        // for every create there is a delete except for the children still present
        to.setCversion(stat.getCversion()*2 - numChildren);
        to.setNumChildren(numChildren);
    }

    /**
     * 获取临时节点对应的client session id ，default：0
     * @param stat
     * @return
     */
    private static long getClientEphemeralOwner(StatPersisted stat) {
        EphemeralType ephemeralType = EphemeralType.get(stat.getEphemeralOwner());
        if (ephemeralType != EphemeralType.NORMAL) {
            return 0;
        }
        return stat.getEphemeralOwner();
    }

    /**
     * 反序列化
     * @param archive
     * @param tag
     * @throws IOException
     */
    synchronized public void deserialize(InputArchive archive, String tag)
            throws IOException {
        archive.startRecord("node");
        data = archive.readBuffer("data");
        acl = archive.readLong("acl");
        stat = new StatPersisted();
        stat.deserialize(archive, "statpersisted");
        archive.endRecord("node");
    }

    /**
     * 序列化
     * @param archive
     * @param tag
     * @throws IOException
     */
    synchronized public void serialize(OutputArchive archive, String tag)
            throws IOException {
        archive.startRecord(this, "node");
        archive.writeBuffer(data, "data");
        archive.writeLong(acl, "acl");
        stat.serialize(archive, "statpersisted");
        archive.endRecord(this, "node");
    }
}
