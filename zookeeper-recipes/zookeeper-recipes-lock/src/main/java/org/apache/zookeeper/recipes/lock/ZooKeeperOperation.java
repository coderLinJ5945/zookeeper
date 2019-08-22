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

import org.apache.zookeeper.KeeperException;

/**
 * 对于zk操作的接口类
 * 使用场景：
 * 针对某个对象相关的操作（增删改查等），但是每个实际执行都有区别，执行结果固定（boolean）
 * 这种时候可以暴露公用的 execute接口，用于开放实现
 * 优点：统一化对象执行，统一化异常
 */
public interface ZooKeeperOperation {
    
    /**
     * 实际执行zk操作的接口，统一化异常，统一化boolean返回操作成功、失败结果
     *
     * @return the result of the operation or null
     * @throws KeeperException
     * @throws InterruptedException
     */
    public boolean execute() throws KeeperException, InterruptedException;
}
