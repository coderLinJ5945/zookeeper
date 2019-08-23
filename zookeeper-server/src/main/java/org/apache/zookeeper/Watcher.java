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

/**
 * This interface specifies the public interface an event handler class must
 * implement. A ZooKeeper client will get various events from the ZooKeeper
 * server it connects to. An application using such a client handles these
 * events by registering a callback object with the client. The callback object
 * is expected to be an instance of a class that implements Watcher interface.
 * 
 */

/**
 * Watcher 接口，基于观察者模式设计
 * 用于观察处理事件
 * client 端将从它连接的zk server 获取各种事件
 * 通过 client应用程序 向 zk server 端注册 回调 对象来处理获取的事件
 * 回调对象是实现 Watcher接口类的实例
 * 个人理解：zookeeper应该是将观察者模式+监听模式结合使用
 *           watcher用于监听服务端事件，
 */
@InterfaceAudience.Public
public interface Watcher {

    /**
     * zk事件的可能状态接口
     */
    @InterfaceAudience.Public
    public interface Event {
        /**
         * ZooKeeper 可能的 事件 状态枚举
         */
        @InterfaceAudience.Public
        public enum KeeperState {
            /** Unused, this state is never generated by the server */
            @Deprecated
            Unknown (-1),

            /** The client is in the disconnected state - it is not connected
             * to any server in the ensemble. */
            Disconnected (0),

            /** Unused, this state is never generated by the server */
            @Deprecated
            NoSyncConnected (1),

            /** The client is in the connected state - it is connected
             * to a server in the ensemble (one of the servers specified
             * in the host connection parameter during ZooKeeper client
             * creation). */
            SyncConnected (3),

            /**
             * Auth failed state
             */
            AuthFailed (4),

            /**
             * The client is connected to a read-only server, that is the
             * server which is not currently connected to the majority.
             * The only operations allowed after receiving this state is
             * read operations.
             * This state is generated for read-only clients only since
             * read/write clients aren't allowed to connect to r/o servers.
             */
            ConnectedReadOnly (5),

            /**
              * SaslAuthenticated: used to notify clients that they are SASL-authenticated,
              * so that they can perform Zookeeper actions with their SASL-authorized permissions.
              */
            SaslAuthenticated(6),

            /** The serving cluster has expired this session. The ZooKeeper
             * client connection (the session) is no longer valid. You must
             * create a new client connection (instantiate a new ZooKeeper
             * instance) if you with to access the ensemble. */
            Expired (-112),
            
            /** 
             * The client has been closed. This state is never generated by
             * the server, but is generated locally when a client calls
             * {@link ZooKeeper#close()} or {@link ZooKeeper#close(int)}
             */
            Closed (7);

            private final int intValue;     // Integer representation of value
                                            // for sending over wire

            KeeperState(int intValue) {
                this.intValue = intValue;
            }

            public int getIntValue() {
                return intValue;
            }

            public static KeeperState fromInt(int intValue) {
                switch(intValue) {
                    case   -1: return KeeperState.Unknown;
                    case    0: return KeeperState.Disconnected;
                    case    1: return KeeperState.NoSyncConnected;
                    case    3: return KeeperState.SyncConnected;
                    case    4: return KeeperState.AuthFailed;
                    case    5: return KeeperState.ConnectedReadOnly;
                    case    6: return KeeperState.SaslAuthenticated;
                    case -112: return KeeperState.Expired;
                    case   7: return KeeperState.Closed;

                    default:
                        throw new RuntimeException("Invalid integer value for conversion to KeeperState");
                }
            }
        }

        /**
         * ZooKeeper 本身节点的状态枚举
         */
        @InterfaceAudience.Public
        public enum EventType {
            None (-1),
            NodeCreated (1),
            NodeDeleted (2),
            NodeDataChanged (3),
            NodeChildrenChanged (4),
            DataWatchRemoved (5),
            ChildWatchRemoved (6);

            private final int intValue;     // Integer representation of value
                                            // for sending over wire

            EventType(int intValue) {
                this.intValue = intValue;
            }

            public int getIntValue() {
                return intValue;
            }

            public static EventType fromInt(int intValue) {
                switch(intValue) {
                    case -1: return EventType.None;
                    case  1: return EventType.NodeCreated;
                    case  2: return EventType.NodeDeleted;
                    case  3: return EventType.NodeDataChanged;
                    case  4: return EventType.NodeChildrenChanged;
                    case  5: return EventType.DataWatchRemoved;
                    case  6: return EventType.ChildWatchRemoved;

                    default:
                        throw new RuntimeException("Invalid integer value for conversion to EventType");
                }
            }           
        }
    }

    /**
     *  watchers 类型的枚举
     *
     */
    @InterfaceAudience.Public
    public enum WatcherType {
        Children(1), Data(2), Any(3);

        // Integer representation of value
        private final int intValue;

        private WatcherType(int intValue) {
            this.intValue = intValue;
        }

        public int getIntValue() {
            return intValue;
        }

        public static WatcherType fromInt(int intValue) {
            switch (intValue) {
            case 1:
                return WatcherType.Children;
            case 2:
                return WatcherType.Data;
            case 3:
                return WatcherType.Any;

            default:
                throw new RuntimeException(
                        "Invalid integer value for conversion to WatcherType");
            }
        }
    }

    /**
     * 观察的事件过程的抽象方法
     * 目前的实现用于zookeeper.recipes相关的锁、选举和队列
     * 抽象方法的使用：无序返回结果，有统一的参数类型
     * @param event
     */
    abstract public void process(WatchedEvent event);
}
