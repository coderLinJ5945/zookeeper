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
package org.apache.zookeeper.common;

import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.Writer;

/**
 *  对文件执行原子写入，new 对象，执行原子写入操作
 *  // TODO: 2019/9/18  需要学习这里怎么保证原子性写入 
 *  如果在写入操作的过程中出现故障，则保留原始文件(如果存在)。

 *  Based on the org.apache.zookeeper.server.quorum.QuorumPeer.writeLongToFile(...) idiom
 *  using the HDFS AtomicFileOutputStream class.
 */
public class AtomicFileWritingIdiom {

    /**
     * 输出流接口声明
     */
    public static interface OutputStreamStatement {

        public void write(OutputStream os) throws IOException;

    }

    /**
     * 写入声明
     */
    public static interface WriterStatement {

        public void write(Writer os) throws IOException;

    }

    public AtomicFileWritingIdiom(File targetFile, OutputStreamStatement osStmt)  throws IOException {
        this(targetFile, osStmt, null);
    }

    public AtomicFileWritingIdiom(File targetFile, WriterStatement wStmt)  throws IOException {
        this(targetFile, null, wStmt);
    }

    /**
     * 核心构造，构造最终目的，写入数据到目标文件
     * @param targetFile 目标文件对象
     * @param osStmt
     * @param wStmt
     * @throws IOException
     */
    private AtomicFileWritingIdiom(File targetFile, OutputStreamStatement osStmt, WriterStatement wStmt)  throws IOException {
        AtomicFileOutputStream out = null;
        boolean error = true;
        try {
            out = new AtomicFileOutputStream(targetFile);
            /**
             * 如果写入缓存对象为null，执行输出流操作
             * 这里是保证写入操作的原子性，以BufferedWriter 缓冲对象进行原子性的写入
             */
            if (wStmt == null) {
                // 执行输出流操作
                osStmt.write(out);
            } else {
                BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(out));
                // 执行写入操作并刷新
                wStmt.write(bw);
                bw.flush();
            }
            out.flush();
            // 数据写入完成了之后，才取消error
            error = false;
        } finally {
            // nothing interesting to do if out == null
            if (out != null) {
                if (error) {
                    /**
                     * 关闭原子文件,写入失败的使用调用该方法
                     * 这里最坏的情况是没有清理tmp文件/资源(fd)
                     * 调用者将被通知(IOException)
                     */
                    out.abort();
                } else {
                    /**
                     * 如果关闭操作(重命名)失败，我们将得到通知。
                     * 最坏的情况下，tmp文件可能仍然存在
                     */
                    IOUtils.closeStream(out);
                }
            }
        }
    }

}
