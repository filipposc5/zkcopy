package com.github.ksprojects.zkcopy.reader;

import com.github.ksprojects.zkcopy.Node;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.log4j.Logger;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;

final class NodeReader implements Runnable {

    private static final Logger LOGGER = Logger.getLogger(NodeReader.class);

    private final Node znode;
    private final ExecutorService pool;
    private final AtomicInteger totalCounter;
    private final AtomicInteger processedCounter;
    private final AtomicInteger failedCounter;
    private final AtomicLong bytesCounter;

    private final AtomicBoolean failed;

    NodeReader(ExecutorService pool, Node znode, AtomicInteger totalCounter, AtomicInteger processedCounter, AtomicInteger failedCounter, AtomicLong bytesCounter,
               AtomicBoolean failed) {
        this.znode = znode;
        this.pool = pool;
        this.totalCounter = totalCounter;
        this.processedCounter = processedCounter;
        this.failedCounter = failedCounter;
        this.bytesCounter = bytesCounter;
        this.failed = failed;
        totalCounter.incrementAndGet();
    }

    @Override
    public void run() {
        try {
            if (failed.get()) {
                return;
            }
            ReaderThread thread = (ReaderThread) Thread.currentThread();
            ZooKeeper zk = thread.getZooKeeper();
            Stat stat = new Stat();
            String path = znode.getAbsolutePath();
            LOGGER.debug("Reading node " + path);
            byte[] data = null;
            try {
                data = zk.getData(path, false, stat);
                if (data != null)
                    bytesCounter.getAndAdd(Long.valueOf(data.length));
            } catch (KeeperException | InterruptedException e) {
                // hack to ignore errors..
                znode.setEphemeral(true);
                throw e;
            }
            if (stat.getEphemeralOwner() != 0) {
                znode.setEphemeral(true);
            }
            znode.setData(data);
            znode.setMtime(stat.getMtime());
            List<String> children = zk.getChildren(path, false);
            for (String child : children) {
                if ("zookeeper".equals(child)) {
                    // reserved
                    continue;
                }
                Node zchild = new Node(znode, child);
                znode.appendChild(zchild);
                pool.execute(new NodeReader(pool, zchild, totalCounter, processedCounter, failedCounter, bytesCounter, failed));
            }
        } catch (KeeperException | InterruptedException e) {
            LOGGER.error("Could not read from remote server", e);
            failedCounter.incrementAndGet();
            //failed.set(true);
        } finally {
            processedCounter.incrementAndGet();
        }
    }

}
