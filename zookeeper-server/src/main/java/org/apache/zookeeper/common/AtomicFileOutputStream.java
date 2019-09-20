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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;


/**
 * // TODO: 2019/9/18 划重点：手动实现原子性的文件输出流
 * 原子性文件输出流类，以下类来至于HDFS
 *
 * 只有完全写入并刷新到磁盘后，才会写入到目标文件中（使用tmp临时文件的方式实现）
 *
 * 注意：在Windows平台上，它不会自动替换目标文件——而是在目标文件被移动到合适的位置之前删除它。
 *
 *
 */
public class AtomicFileOutputStream extends FilterOutputStream {
    /**
     * 临时文件后缀名
     */
    private static final String TMP_EXTENSION = ".tmp";

    private final static Logger LOG = LoggerFactory
            .getLogger(AtomicFileOutputStream.class);

    /**
     * 源文件对象
     */
    private final File origFile;

    /**
     * 存储临时文件对象
     */
    private final File tmpFile;

    /**
     *  构造器：构造源文件和临时文件对象
     * @param f
     * @throws FileNotFoundException
     */
    public AtomicFileOutputStream(File f) throws FileNotFoundException {
        super(new FileOutputStream(new File(f.getParentFile(), f.getName()
                + TMP_EXTENSION)));
        /**
         * 下面的代码必须重复，因为我们不能在调用super之前分配任何东西
         * super机制决定
         */
        origFile = f.getAbsoluteFile();
        tmpFile = new File(f.getParentFile(), f.getName() + TMP_EXTENSION)
                .getAbsoluteFile();
    }

    /**
     * 重写 FilterOutputStream 文件输出流的写入方法
     * 原因：FilterOutputStream 默认的写入是一个byte一个byte的写入，
     *       这里重写调用OutputStream 的write方法是为了优化写入
     * @param b
     * @param off
     * @param len
     * @throws IOException
     */
    @Override
    public void write(byte b[], int off, int len) throws IOException {
        out.write(b, off, len);
    }

    /**
     * 关闭此输出流并释放与此流关联的任何系统资源
     * 这里重写该方法是为了保证文件的原子性
     * @throws IOException
     */
    @Override
    public void close() throws IOException {
        /**
         * 初始化 triedToClose 开关和关闭结果success ：false
         */
        boolean triedToClose = false, success = false;
        try {
            /**
             * 刷新此输出流，并强制将任何已缓存的输出字节写入该流
             * 如果不出任何异常，改变triedToClose 和success 状态为true
             */
            flush();
            ((FileOutputStream) out).getFD().sync();

            triedToClose = true;
            super.close();
            success = true;
        } finally {
            /**
             *  如果关闭正常：重命名临时文件为源文件
             *  如果关闭异常：试着关闭它，以不泄漏FD（删除失败的临时文件）
             *
             */
            if (success) {
                boolean renamed = tmpFile.renameTo(origFile);
                if (!renamed) {
                    // On windows, renameTo does not replace.
                    if (!origFile.delete() || !tmpFile.renameTo(origFile)) {
                        throw new IOException(
                                "Could not rename temporary file " + tmpFile
                                        + " to " + origFile);
                    }
                }
            } else {
                if (!triedToClose) {
                    // If we failed when flushing, try to close it to not leak
                    // an FD
                    IOUtils.closeStream(out);
                }
                // close wasn't successful, try to delete the tmp file
                if (!tmpFile.delete()) {
                    LOG.warn("Unable to delete tmp file " + tmpFile);
                }
            }
        }
    }

    /**
     * 关闭原子文件,应该在写入失败的使用调用该方法
     */
    public void abort() {
        try {
            super.close();
        } catch (IOException ioe) {
            LOG.warn("Unable to abort file " + tmpFile, ioe);
        }
        if (!tmpFile.delete()) {
            LOG.warn("Unable to delete tmp file during abort " + tmpFile);
        }
    }
}
