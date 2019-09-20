# Apache ZooKeeper
![alt text](https://zookeeper.apache.org/images/zookeeper_small.gif "ZooKeeper")

For the latest information about Apache ZooKeeper, please visit our website at:

   http://zookeeper.apache.org/

and our wiki, at:

   https://cwiki.apache.org/confluence/display/ZOOKEEPER

## Packaging/release artifacts

Either downloaded from https://zookeeper.apache.org/releases.html or
found in zookeeper-assembly/target directory after building the project with maven.

    apache-zookeeper-[version].tar.gz

        Contains all the source files which can be built by running:
        mvn clean install

        To generate an aggregated apidocs for zookeeper-server and zookeeper-jute:
        mvn javadoc:aggregate
        (generated files will be at target/site/apidocs)

    apache-zookeeper-[version]-bin.tar.gz

        Contains all the jar files required to run ZooKeeper
        Full documentation can also be found in the docs folder

As of version 3.5.5, the parent, zookeeper and zookeeper-jute artifacts
are deployed to the central repository after the release
is voted on and approved by the Apache ZooKeeper PMC:

  https://repo1.maven.org/maven2/org/apache/zookeeper/zookeeper/



## Java 8

If you are going to compile with Java 1.8, you should use a
recent release at u211 or above. 

## Contributing
We always welcome new contributors to the project! See [How to Contribute](https://cwiki.apache.org/confluence/display/ZOOKEEPER/HowToContribute) for details on how to submit patch through pull request and our contribution workflow.

## 核心源码阅读
1. server 启动（单机）
2. zookeeper client 和 server 交互（单机）
3. leader选主模式（集群）
4. 数据一致性的具体代码实现（集群）

## 个人理解
1. zookeeper 之所以运用到dubbo、kafka等各种分布式服务中的原因

    a. 分布式系统的痛点：单点故障，zookeeper 的选主模式可以很好的在单点故障之后进行重新分配，恢复现有的分布式服务
    b. 一致性算法，处理分布式中的需要保持一致性的原则




