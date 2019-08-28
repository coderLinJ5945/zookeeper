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

import org.apache.zookeeper.ClientCnxn.EndOfStreamException;
import org.apache.zookeeper.ClientCnxn.Packet;
import org.apache.zookeeper.ZooDefs.OpCode;
import org.apache.zookeeper.client.ZKClientConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.LinkedBlockingDeque;
/**
 *  Socket 到持久化zk数据类
 *  该类的连接 {@link #connect(InetSocketAddress)}.
 *  该类的消息传输入口{@link #doTransport}.
 *
 *  ClientCnxnSocketNIO 是 ClientCnxnSocket 的具体实现 非阻塞IO流的实现
 *  非阻塞IO的实现，其实是使用的缓冲区实现，一般用于实时通讯
 *  这里需要区分的是和 ClientCnxnSocketNetty的区别？
 *  // TODO: 2019/8/27 前提 java.nio核心 
 *  // TODO: 2019/8/27  同步 、异步、 阻塞 和非阻塞 demo实现
 *  非阻塞demo ： nonblocking
 *
 */
public class ClientCnxnSocketNIO extends ClientCnxnSocket {
    private static final Logger LOG = LoggerFactory
            .getLogger(ClientCnxnSocketNIO.class);

    /**
     *  java.nio 核心之一 Selector 选择器
     *  这里用于 SocketChannel 的注册
     *  socket 通信操作步骤：
     *  1. 初始化建立选择器
     *  2. 构造 socket 通道
     *  3. 注册
     *  4. selector.select(); 等待socket通道的事件
     */
    private final Selector selector = Selector.open();

    /**
     * SelectableChannel与Selector的注册的令牌
     */
    private SelectionKey sockKey;

    /** 本地socket地址 */
    private SocketAddress localSocketAddress;
    /** 远程socket地址 */
    private SocketAddress remoteSocketAddress;

    /**
     * 通过 ZKClientConfig 配置初始化 ClientCnxnSocketNIO 信息
     * @param clientConfig
     * @throws IOException
     */
    ClientCnxnSocketNIO(ZKClientConfig clientConfig) throws IOException {
        this.clientConfig = clientConfig;
        initProperties();
    }

    @Override
    boolean isConnected() {
        return sockKey != null;
    }
    
    /**
     * 由{@link #doTransport} client传输数据方法调用
     * B. 执行读写操作
     * @return true if a packet was received
     * @throws InterruptedException
     * @throws IOException
     */
    void doIO(List<Packet> pendingQueue, ClientCnxn cnxn)
      throws InterruptedException, IOException {
        SocketChannel sock = (SocketChannel) sockKey.channel();
        if (sock == null) {
            throw new IOException("Socket is null!");
        }
        // socket通道为可读状态
        if (sockKey.isReadable()) {
            //读取缓冲区数据
            int rc = sock.read(incomingBuffer);
            // rc < 0 无法从服务器sessionid读取其他数据
            if (rc < 0) {
                throw new EndOfStreamException(
                        "Unable to read additional data from server sessionid 0x"
                                + Long.toHexString(sessionId)
                                + ", likely server has closed socket");
            }
            // 判断当前位置到极限位置之间是否有数据
            // TODO: 2019/8/28 读取的细节没看明白 
            if (!incomingBuffer.hasRemaining()) {
                //如果没有数据，说明数据读完了
                incomingBuffer.flip();
                if (incomingBuffer == lenBuffer) {
                    recvCount.getAndIncrement();
                    readLength();
                } else if (!initialized) {
                    readConnectResult();
                    enableRead();
                    if (findSendablePacket(outgoingQueue,
                            sendThread.tunnelAuthInProgress()) != null) {
                        // Since SASL authentication has completed (if client is configured to do so),
                        // outgoing packets waiting in the outgoingQueue can now be sent.
                        enableWrite();
                    }
                    lenBuffer.clear();
                    incomingBuffer = lenBuffer;
                    updateLastHeard();
                    initialized = true;
                } else {
                    sendThread.readResponse(incomingBuffer);
                    lenBuffer.clear();
                    incomingBuffer = lenBuffer;
                    updateLastHeard();
                }
            }
        }

        // socket通道为可写入状态
        if (sockKey.isWritable()) {
            Packet p = findSendablePacket(outgoingQueue,
                    sendThread.tunnelAuthInProgress());

            if (p != null) {
                updateLastSend();
                // If we already started writing p, p.bb will already exist
                if (p.bb == null) {
                    if ((p.requestHeader != null) &&
                            (p.requestHeader.getType() != OpCode.ping) &&
                            (p.requestHeader.getType() != OpCode.auth)) {
                        p.requestHeader.setXid(cnxn.getXid());
                    }
                    p.createBB();
                }
                sock.write(p.bb);
                if (!p.bb.hasRemaining()) {
                    sentCount.getAndIncrement();
                    outgoingQueue.removeFirstOccurrence(p);
                    if (p.requestHeader != null
                            && p.requestHeader.getType() != OpCode.ping
                            && p.requestHeader.getType() != OpCode.auth) {
                        synchronized (pendingQueue) {
                            pendingQueue.add(p);
                        }
                    }
                }
            }
            if (outgoingQueue.isEmpty()) {
                // No more packets to send: turn off write interest flag.
                // Will be turned on later by a later call to enableWrite(),
                // from within ZooKeeperSaslClient (if client is configured
                // to attempt SASL authentication), or in either doIO() or
                // in doTransport() if not.
                disableWrite();
            } else if (!initialized && p != null && !p.bb.hasRemaining()) {
                // On initial connection, write the complete connect request
                // packet, but then disable further writes until after
                // receiving a successful connection response.  If the
                // session is expired, then the server sends the expiration
                // response and immediately closes its end of the socket.  If
                // the client is simultaneously writing on its end, then the
                // TCP stack may choose to abort with RST, in which case the
                // client would never receive the session expired event.  See
                // http://docs.oracle.com/javase/6/docs/technotes/guides/net/articles/connection_release.html
                disableWrite();
            } else {
                // Just in case
                enableWrite();
            }
        }
    }

    /**
     * doIO方法调用
     * C.从 outgoingQueue 队列中获取 Packet
     * @param outgoingQueue
     * @param tunneledAuthInProgres
     * @return
     */
    private Packet findSendablePacket(LinkedBlockingDeque<Packet> outgoingQueue,
                                      boolean tunneledAuthInProgres) {
        if (outgoingQueue.isEmpty()) {
            return null;
        }
        // If we've already starting sending the first packet, we better finish
        if (outgoingQueue.getFirst().bb != null || !tunneledAuthInProgres) {
            return outgoingQueue.getFirst();
        }
        // Since client's authentication with server is in progress,
        // send only the null-header packet queued by primeConnection().
        // This packet must be sent so that the SASL authentication process
        // can proceed, but all other packets should wait until
        // SASL authentication completes.
        Iterator<Packet> iter = outgoingQueue.iterator();
        while (iter.hasNext()) {
            Packet p = iter.next();
            if (p.requestHeader == null) {
                // We've found the priming-packet. Move it to the beginning of the queue.
                iter.remove();
                outgoingQueue.addFirst(p);
                return p;
            } else {
                // Non-priming packet: defer it until later, leaving it in the queue
                // until authentication completes.
                LOG.debug("deferring non-priming packet {} until SASL authentation completes.", p);
            }
        }
        return null;
    }

    /**
     * 清理资源
     * 在重连或关闭的时候调用
     */
    @Override
    void cleanup() {
        if (sockKey != null) {
            SocketChannel sock = (SocketChannel) sockKey.channel();
            sockKey.cancel();
            try {
                sock.socket().shutdownInput();
            } catch (IOException e) {
                if (LOG.isDebugEnabled()) {
                    LOG.debug("Ignoring exception during shutdown input", e);
                }
            }
            try {
                sock.socket().shutdownOutput();
            } catch (IOException e) {
                if (LOG.isDebugEnabled()) {
                    LOG.debug("Ignoring exception during shutdown output",
                            e);
                }
            }
            try {
                sock.socket().close();
            } catch (IOException e) {
                if (LOG.isDebugEnabled()) {
                    LOG.debug("Ignoring exception during socket close", e);
                }
            }
            try {
                sock.close();
            } catch (IOException e) {
                if (LOG.isDebugEnabled()) {
                    LOG.debug("Ignoring exception during channel close", e);
                }
            }
        }
        try {
            Thread.sleep(100);
        } catch (InterruptedException e) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("SendThread interrupted during sleep, ignoring");
            }
        }
        sockKey = null;
    }
 
    @Override
    void close() {
        try {
            if (LOG.isTraceEnabled()) {
                LOG.trace("Doing client selector close");
            }
            selector.close();
            if (LOG.isTraceEnabled()) {
                LOG.trace("Closed client selector");
            }
        } catch (IOException e) {
            LOG.warn("Ignoring exception during selector close", e);
        }
    }
    
    /**
     * 创建 socket channel
     * @return the created socket channel
     * @throws IOException
     */
    SocketChannel createSock() throws IOException {
        SocketChannel sock;
        sock = SocketChannel.open();
        sock.configureBlocking(false);
        sock.socket().setSoLinger(false, -1);
        sock.socket().setTcpNoDelay(true);
        return sock;
    }

    /**
     * 注册 连接
     * @param sock the {@link SocketChannel} 
     * @param addr the address of remote host
     * @throws IOException
     */
    void registerAndConnect(SocketChannel sock, InetSocketAddress addr) 
    throws IOException {
        /** socket 通信操作步骤：
         *  1. 初始化建立选择器
         *  2. 初始化socket通道
         *  3. 注册
         */
        //1. 注册socket 通道到选择器，并获取socket秘钥
        sockKey = sock.register(selector, SelectionKey.OP_CONNECT);
        //2. 最终执行 socket 连接
        boolean immediateConnect = sock.connect(addr);
        /**
         * 3. 如果连接上，设置session、watches和身份验证
         */
        if (immediateConnect) {
            sendThread.primeConnection();
        }
    }

    /**
     * 创建client 连接：该类执行的入口
     * @param addr
     * @throws IOException
     */
    @Override
    void connect(InetSocketAddress addr) throws IOException {
        /**
         * 1. 构造 socket 通道
         */
        SocketChannel sock = createSock();
        try {
            /**
             * 2. 注册 socket 通道 并尝试连接
             */
           registerAndConnect(sock, addr);
      } catch (IOException e) {
            LOG.error("Unable to open socket to " + addr);
            sock.close();
            throw e;
        }
        initialized = false;

        /*
         * Reset incomingBuffer
         */
        lenBuffer.clear();
        incomingBuffer = lenBuffer;
    }

    /**
     * Returns the address to which the socket is connected.
     * 
     * @return ip address of the remote side of the connection or null if not
     *         connected
     */
    @Override
    SocketAddress getRemoteSocketAddress() {
        return remoteSocketAddress;
    }

    /**
     * Returns the local address to which the socket is bound.
     * 
     * @return ip address of the remote side of the connection or null if not
     *         connected
     */
    @Override
    SocketAddress getLocalSocketAddress() {
        return localSocketAddress;
    }
    
    private void updateSocketAddresses() {
        Socket socket = ((SocketChannel) sockKey.channel()).socket();
        localSocketAddress = socket.getLocalSocketAddress();
        remoteSocketAddress = socket.getRemoteSocketAddress();
    }

    @Override
    void packetAdded() {
        wakeupCnxn();
    }

    @Override
    void onClosing() {
        wakeupCnxn();
    }

    private synchronized void wakeupCnxn() {
        selector.wakeup();
    }

    /**
     * 由 {@link org.apache.zookeeper.ClientCnxn.SendThread} 的线程的run方法执行调用
     * A、执行client 到server 的数据运输
     * @param waitTimeOut timeout in blocking wait. Unit in MilliSecond.
     * @param pendingQueue These are the packets that have been sent and
     *                     are waiting for a response.
     * @param cnxn
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    void doTransport(int waitTimeOut, List<Packet> pendingQueue, ClientCnxn cnxn)
            throws IOException, InterruptedException {
        /**
         * 此方法执行阻止selection operation 。
         * 只有在选择了至少一个通道之后，才会返回此选择器的wakeup方法,
         * 当前线程被中断，或给定的超时期限到期，以先到者为准。
         */
        selector.select(waitTimeOut);

        Set<SelectionKey> selected;
        //锁定当前的调用线程，这里的this为SendThread对象
        synchronized (this) {
            selected = selector.selectedKeys();
        }
        /**
         * 以下代码直到我们回到select之前都是非阻塞的
         * 所以时间实际上是一个常数。这就是为什么我们只需要做一次
         *
         * 反向理解，当有select时，以下代码为阻塞状态，逐步执行
         */
        //更新当前时间
        updateNow();
        /**
         * 遍历 selector 中所有通道的 SelectionKey,
         * 对该选择器锁监听的通道的连接、读写 IO的处理
         */
        for (SelectionKey k : selected) {
            SocketChannel sc = ((SocketChannel) k.channel());
            //如果就绪操作集和连接个数都不为0，这里是判断连接的操作
            if ((k.readyOps() & SelectionKey.OP_CONNECT) != 0) {
                /**
                 *  如果已经完成socket 通道的连接，执行以下操作：
                 *  1. 更新最后一次的发送时间为now
                 *  2. 更新socket和address
                 *  3. 设置 session、watches 和身份验证
                 */
                if (sc.finishConnect()) {
                    updateLastSendAndHeard();
                    updateSocketAddresses();
                    sendThread.primeConnection();
                    System.out.println("连接成功");
                }
                /**
                 *  这里判断如何有读写操作的 SelectionKey，则执行相应的读写IO
                 */
            } else if ((k.readyOps() & (SelectionKey.OP_READ | SelectionKey.OP_WRITE)) != 0) {
                // 执行读写IO
                doIO(pendingQueue, cnxn);
            }

        }

        if (sendThread.getZkState().isConnected()) {
            if (findSendablePacket(outgoingQueue,
                    sendThread.tunnelAuthInProgress()) != null) {
                enableWrite();
            }
        }
        // 选择器执行完成之后，最后清除selector 中的 selectedKeys
        selected.clear();
    }

    //TODO should this be synchronized?
    @Override
    void testableCloseSocket() throws IOException {
        LOG.info("testableCloseSocket() called");
        // sockKey may be concurrently accessed by multiple
        // threads. We use tmp here to avoid a race condition
        SelectionKey tmp = sockKey;
        if (tmp!=null) {
           ((SocketChannel) tmp.channel()).socket().close();
        }
    }

    @Override
    void saslCompleted() {
        enableWrite();
    }

    synchronized void enableWrite() {
        int i = sockKey.interestOps();
        if ((i & SelectionKey.OP_WRITE) == 0) {
            sockKey.interestOps(i | SelectionKey.OP_WRITE);
        }
    }

    private synchronized void disableWrite() {
        int i = sockKey.interestOps();
        if ((i & SelectionKey.OP_WRITE) != 0) {
            sockKey.interestOps(i & (~SelectionKey.OP_WRITE));
        }
    }

    synchronized private void enableRead() {
        int i = sockKey.interestOps();
        if ((i & SelectionKey.OP_READ) == 0) {
            sockKey.interestOps(i | SelectionKey.OP_READ);
        }
    }

    @Override
    void connectionPrimed() {
        sockKey.interestOps(SelectionKey.OP_READ | SelectionKey.OP_WRITE);
    }

    Selector getSelector() {
        return selector;
    }

    @Override
    void sendPacket(Packet p) throws IOException {
        SocketChannel sock = (SocketChannel) sockKey.channel();
        if (sock == null) {
            throw new IOException("Socket is null!");
        }
        p.createBB();
        ByteBuffer pbb = p.bb;
        sock.write(pbb);
    }
}
