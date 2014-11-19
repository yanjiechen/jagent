package com.sohu.cloudno.comm;

import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.Logger;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;

/**
 * Acts as the single ZooKeeper Watcher. One instance of this is instantiated
 * for client process.
 * 
 * <p>
 * This is the only class that implements {@link Watcher}. Other internal
 * classes which need to be notified of ZooKeeper events must register with the
 * local instance of this watcher via {@link #registerListener}.
 * 
 * <p>
 * This class also holds and manages the connection to ZooKeeper. Code to deal
 * with connection related events and exceptions are handled here. fixed by
 * yilai 建立连接修改了代码
 */
public class ZooKeeperWatcher implements Watcher, Abortable {
    private static final Logger LOG = Logger.getLogger(ZooKeeperWatcher.class);
    // listeners to be notified
    private final List<ZooKeeperListener> listeners = new CopyOnWriteArrayList<ZooKeeperListener>();

    // zookeeper connection
    private ZooKeeper zooKeeper;
    // abortable in case of zk failure
    private Abortable abortable;

    // Identifiier for this watcher (for logging only). Its made of the prefix
    // passed on construction and the zookeeper sessionid.
    private String identifier;

    private Conf conf;

    private CountDownLatch connectedSignal = new CountDownLatch(1);

    // 已经设置了watcher的znodes集合
    private Set<String> unassignedNodes = new HashSet<String>();

    /**
     * 建立 ZookeepWatcher,允许abortable为null fixed by yilai
     * 
     * @param conf
     *            配置文件
     * @param descriptor
     *            描叙信息
     * @throws IOException
     * @throws ZooKeeperConnectionException
     * @throws InterruptedException
     */
    /*
     * public ZooKeeperWatcher(FileConfig conf,String descriptor ) throws
     * IOException, ZooKeeperConnectionException,InterruptedException { new
     * ZooKeeperWatcher(conf,descriptor,null); }
     */

    /**
     * Instantiate a ZooKeeper connection and watcher.
     * 
     * @param descriptor
     *            Descriptive string that is added to zookeeper sessionid and
     *            used as identifier for this instance.
     * @throws IOException
     * @throws ZooKeeperConnectionException
     */
    public ZooKeeperWatcher(Conf conf, String descriptor, Abortable abortable)
            throws IOException, ZooKeeperConnectionException,
            InterruptedException {

        this.conf = conf;

        // Identifier will get the sessionid appended later below down when we
        // handle the syncconnect event.
        this.identifier = descriptor;
        this.abortable = abortable;
        this.zooKeeper = ZKUtil.conn(this.conf, this);
        // fixed by yilai,一直等待连接成功或者到达超时
        if (!connectedSignal
                .await(this.conf.getInt("hbase.zookeeper.recoverable.waittime",
                        30000), TimeUnit.MILLISECONDS)) {
            // 连接超时了
            try {
                // If we don't close it, the zk connection managers won't be
                // killed
                this.zooKeeper.close();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                LOG.warn("Interrupted while closing", e);
            }

            throw new ZooKeeperConnectionException(
                    "JAgent is able to connect to"
                            + " ZooKeeper but the connection closes immediately. This could be"
                            + " a sign that the server has too many connections (30 is the"
                            + " default). Consider inspecting your ZK server logs for that"
                            + " error ");
        }
        ;
        LOG.debug("zk has connected " + this.zooKeeper + " " + this.toString());
    }

    /**
     * Method called from ZooKeeper for events and connection status.
     * 
     * Valid events are passed along to listeners. Connection status changes are
     * dealt with locally.
     */
    @Override
    public void process(WatchedEvent event) {
        LOG.info(prefix("Received ZooKeeper Event, type=" + event.getType()
                + ", state=" + event.getState() + ", path=" + event.getPath()));

        switch (event.getType()) {

        // If event type is NONE, this is a connection status change
        case None: {
            connectionEvent(event);
            break;
        }

        // Otherwise pass along to the listeners

        case NodeCreated: {
            for (ZooKeeperListener listener : listeners) {
                listener.nodeCreated(event.getPath());
            }
            break;
        }

        case NodeDeleted: {
            for (ZooKeeperListener listener : listeners) {
                listener.nodeDeleted(event.getPath());
            }
            break;
        }

        case NodeDataChanged: {
            for (ZooKeeperListener listener : listeners) {
                listener.nodeDataChanged(event.getPath());
            }
            break;
        }

        case NodeChildrenChanged: {
            for (ZooKeeperListener listener : listeners) {
                listener.nodeChildrenChanged(event.getPath());
            }
            break;
        }
        }
    }

    /**
     * Called when there is a connection-related event via the Watcher callback.
     * 
     * If Disconnected or Expired, this should shutdown the cluster. But, since
     * we send a KeeperException.SessionExpiredException along with the abort
     * call, it's possible for the Abortable to catch it and try to create a new
     * session with ZooKeeper. This is what the client does in HCM.
     * 
     * @param event
     */
    private void connectionEvent(WatchedEvent event) {
        switch (event.getState()) {
        case SyncConnected:
            // Now, this callback can be invoked before the this.zookeeper is
            // set.
            // Wait a little while.
            /*
             * long finished = System.currentTimeMillis() +
             * Constant.zookeeperSyncWait; while (System.currentTimeMillis() <
             * finished) { sleep(1); if (this.zooKeeper != null) break; } if
             * (this.zooKeeper == null) {
             * LOG.error("ZK is null on connection event -- see stack trace " );
             * throw new NullPointerException("ZK is null"); }
             */
            identifier += "-0x" + Long.toHexString(zooKeeper.getSessionId());
            // Update our identifier. Otherwise ignore.
            LOG.debug(identifier + " connected");
            // fixed by yilai,通知连接成功
            if (connectedSignal.getCount() > 0)
                connectedSignal.countDown();
            break;

        // Abort the server if Disconnected or Expired
        // TODO: any reason to handle these two differently?
        case Disconnected:
            LOG.info(prefix("Received Disconnected from ZooKeeper, ignoring"));
            break;

        case Expired:
            String why = prefix(this.identifier
                    + " received expired from ZooKeeper, aborting");
            // TODO: One thought is to add call to ZooKeeperListener so say,
            // ZooKeperNodeTracker can zero out its data values.
            if (abortable != null)
                abortable.abort(why,
                        new KeeperException.SessionExpiredException());
            break;
        default:
            break;
        }
    }

    /**
     * Register the specified listener to receive ZooKeeper events.
     * 
     * @param listener
     */
    public void registerListener(ZooKeeperListener listener) {
        listeners.add(listener);
    }

    /**
     * Register the specified listener to receive ZooKeeper events and add it as
     * the first in the list of current listeners.
     * 
     * @param listener
     */
    public void registerListenerFirst(ZooKeeperListener listener) {
        listeners.add(0, listener);
    }

    /**
     * Adds this instance's identifier as a prefix to the passed
     * <code>str</code>
     * 
     * @param str
     *            String to amend.
     * @return A new string with this instance's identifier as prefix: e.g. if
     *         passed 'hello world', the returned string could be
     */
    public String prefix(final String str) {
        return this.toString() + " " + str;
    }

    /**
     * 睡眠指定的毫秒数
     * 
     * @param millis
     *            How long to sleep for in milliseconds.
     */
    public void sleep(int millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    /**
     * 判断是否重试到了了指定时间
     * 
     * @param finished
     *            指定时间
     * @return true,指定时间没到
     */
    // private boolean isFinishedRetryingRecoverable(final long finished) {
    // return System.currentTimeMillis() < finished;
    // }

    @Override
    public String toString() {
        return identifier;
    }

    /**
     * Get the connection to ZooKeeper.
     * 
     * @return connection reference to zookeeper
     */
    public ZooKeeper getZooKeeper() {
        return zooKeeper;
    }

    /**
     * Forces a synchronization of this ZooKeeper client connection.
     * <p>
     * Executing this method before running other methods will ensure that the
     * subsequent operations are up-to-date and consistent as of the time that
     * the sync is complete.
     * <p>
     * This is used for compareAndSwap type operations where we need to read the
     * data of an existing node and delete or transition that node, utilizing
     * the previously read version and data. We want to ensure that the version
     * read is up-to-date from when we begin the operation.
     */
    public void sync(String path) {
        zooKeeper.sync(path, null, null);
    }

    /**
     * Handles KeeperExceptions in client calls.
     * 
     * This may be temporary but for now this gives one place to deal with
     * these.
     * 
     * TODO: Currently this method rethrows the exception to let the caller
     * handle
     * 
     * @param ke
     * @throws KeeperException
     */
    public void keeperException(KeeperException e) throws KeeperException {
        LOG.error(
                prefix("Received unexpected KeeperException, re-throwing exception"),
                e);
        throw e;
    }

    /**
     * Handles InterruptedExceptions in client calls.
     * 
     * This may be temporary but for now this gives one place to deal with
     * these.
     * 
     * TODO: Currently, this method does nothing. Is this ever expected to
     * happen? Do we abort or can we let it run? Maybe this should be logged as
     * WARN? It shouldn't happen?
     * 
     * @param ie
     */
    public void interruptedException(InterruptedException e) {
        LOG.debug(prefix("Received InterruptedException, doing nothing here"),
                e);
        // At least preserver interrupt.
        Thread.currentThread().interrupt();
        // no-op
    }

    /**
     * Close the connection to ZooKeeper.
     * 
     * @throws InterruptedException
     */
    public void close() {
        // LOG.info(this.identifier + " begin to close");
        try {
            if (zooKeeper != null) {
                zooKeeper.close();
                LOG.info(this.identifier + " closed");
                // super.close();
            }
        } catch (InterruptedException e) {
        }
    }

    /**
     * Get the set of already watched unassigned nodes.
     * 
     * @return Set of Nodes.
     */
    public Set<String> getNodes() {
        return unassignedNodes;
    }

    @Override
    public void abort(String why, Throwable e) {
        if (abortable != null)
            abortable.abort(why, e);
    }
}