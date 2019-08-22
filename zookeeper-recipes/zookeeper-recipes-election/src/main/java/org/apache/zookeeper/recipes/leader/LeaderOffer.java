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

import java.io.Serializable;
import java.util.Comparator;

/**
 * leader 节点的  id / path
 * id ：ZooKeeper分配的顺序节点id
 * nodePath ：到ZNode的绝对路径
 * hostName ：leader 所在的hostname
 *
 * 实际选主算法，对于程序来说就是创建 LeaderOffer 的实例
 */
public class LeaderOffer {

  private Integer id;
  private String nodePath;
  private String hostName;

  public LeaderOffer() {
    // Default constructor
  }

  public LeaderOffer(Integer id, String nodePath, String hostName) {
    this.id = id;
    this.nodePath = nodePath;
    this.hostName = hostName;
  }

  @Override
  public String toString() {
    return "{ id:" + id + " nodePath:" + nodePath + " hostName:" + hostName
        + " }";
  }

  public Integer getId() {
    return id;
  }

  public void setId(Integer id) {
    this.id = id;
  }

  public String getNodePath() {
    return nodePath;
  }

  public void setNodePath(String nodePath) {
    this.nodePath = nodePath;
  }

  public String getHostName() {
    return hostName;
  }

  public void setHostName(String hostName) {
    this.hostName = hostName;
  }

  /**
   * Compare two instances of {@link LeaderOffer} using only the {code}id{code}
   * member.
   */
  public static class IdComparator
          implements Comparator<LeaderOffer>, Serializable {

    @Override
    public int compare(LeaderOffer o1, LeaderOffer o2) {
      return o1.getId().compareTo(o2.getId());
    }

  }

}
