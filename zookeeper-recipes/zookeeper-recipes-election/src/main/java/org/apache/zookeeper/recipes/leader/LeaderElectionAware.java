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

import org.apache.zookeeper.recipes.leader.LeaderElectionSupport.EventType;

/**
 * 一个接口被clients 实现，用于接收选举事件
 */
public interface LeaderElectionAware {

  /**
   *  服务状态改变时调用此接口 ：OFFER_START, OFFER_COMPLETE, DETERMINE_START, DETERMINE_COMPLETE 等
   * @param eventType
   */
  public void onElectionEvent(EventType eventType);

}
