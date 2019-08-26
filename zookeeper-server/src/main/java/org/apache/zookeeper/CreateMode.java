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
package org.apache.zookeeper;

import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/***
 *  创建 znode 的节点类型枚举类
 */
@InterfaceAudience.Public
public enum CreateMode {

    /**
     * 永久节点，client断开连接，也不会自动删除
     */
    PERSISTENT (0, false, false, false, false),
    /**
     * 永久节点，序列化
     * client断开连接，不会自动删除，nodeName将添加一个单调递增的数字
     */
    PERSISTENT_SEQUENTIAL (2, false, true, false, false),
    /**
     * 临时节点，client断开连接，自动删除
     */
    EPHEMERAL (1, true, false, false, false),
    /**
     * 临时节点，client断开连接，自动删除，nodeName将添加一个单调递增的数字
     */
    EPHEMERAL_SEQUENTIAL (3, true, true, false, false),
    /**
     * 容器节点，特殊通途节点，例如 leader, lock 等
     * 当删除容器的最后一个子元素时，容器将成为将来某个时候服务器要删除的候选对象。
     */
    CONTAINER (4, false, false, true, false),
    /**
     * client 断开，znode不会自动删除
     * 但是，如果在给定的TTL中没有修改znode，那么一旦它没有子节点，它就会被删除
     * （TTL: time of life 存活时间）
     */
    PERSISTENT_WITH_TTL(5, false, false, false, true),
    /**
     * 在客户端断开连接时，znode不会自动删除，它的名称将添加一个单调递增的数字。
     * 但是，如果在给定的TTL中没有修改znode，那么一旦它没有子节点，它就会被删除。
     */
    PERSISTENT_SEQUENTIAL_WITH_TTL(6, false, true, false, true);

    private static final Logger LOG = LoggerFactory.getLogger(CreateMode.class);

    private boolean ephemeral;
    /* 用于判断节点顺序？*/
    private boolean sequential;
    private final boolean isContainer;
    private int flag;
    /* TTL ？临时节点？*/
    private boolean isTTL;

    CreateMode(int flag, boolean ephemeral, boolean sequential,
               boolean isContainer, boolean isTTL) {
        this.flag = flag;
        this.ephemeral = ephemeral;
        this.sequential = sequential;
        this.isContainer = isContainer;
        this.isTTL = isTTL;
    }

    public boolean isEphemeral() { 
        return ephemeral;
    }

    public boolean isSequential() { 
        return sequential;
    }

    public boolean isContainer() {
        return isContainer;
    }

    public boolean isTTL() {
        return isTTL;
    }

    public int toFlag() {
        return flag;
    }

    /**
     * Map an integer value to a CreateMode value
     */
    static public CreateMode fromFlag(int flag) throws KeeperException {
        switch(flag) {
        case 0: return CreateMode.PERSISTENT;

        case 1: return CreateMode.EPHEMERAL;

        case 2: return CreateMode.PERSISTENT_SEQUENTIAL;

        case 3: return CreateMode.EPHEMERAL_SEQUENTIAL ;

        case 4: return CreateMode.CONTAINER;

        case 5: return CreateMode.PERSISTENT_WITH_TTL;

        case 6: return CreateMode.PERSISTENT_SEQUENTIAL_WITH_TTL;

        default:
            String errMsg = "Received an invalid flag value: " + flag
                    + " to convert to a CreateMode";
            LOG.error(errMsg);
            throw new KeeperException.BadArgumentsException(errMsg);
        }
    }

    /**
     * Map an integer value to a CreateMode value
     */
    static public CreateMode fromFlag(int flag, CreateMode defaultMode) {
        switch(flag) {
            case 0:
                return CreateMode.PERSISTENT;

            case 1:
                return CreateMode.EPHEMERAL;

            case 2:
                return CreateMode.PERSISTENT_SEQUENTIAL;

            case 3:
                return CreateMode.EPHEMERAL_SEQUENTIAL;

            case 4:
                return CreateMode.CONTAINER;

            case 5:
                return CreateMode.PERSISTENT_WITH_TTL;

            case 6:
                return CreateMode.PERSISTENT_SEQUENTIAL_WITH_TTL;

            default:
                return defaultMode;
        }
    }
}
