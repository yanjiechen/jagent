package com.sohu.cloudno.comm;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Logger;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.KeeperException.NoNodeException;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;

/**
 * Internal HBase utility class for ZooKeeper.
 * 
 * <p>
 * Contains only static methods and constants.
 * 
 * <p>
 * Methods all throw {@link KeeperException} if there is an unexpected zookeeper
 * exception, so callers of these methods must handle appropriately. If ZK is
 * required for the operation, the server will need to be aborted.
 */
public class ZKUtil {
    private static final Logger LOG = Logger.getLogger(ZKUtil.class);
    private static final char ZNODE_PATH_SEPARATOR = '/';

    /**
     * Creates a new connection to ZooKeeper, pulling settings and ensemble
     * config from the specified configuration object using methods from
     * {@link ZKConfig}.
     * 
     * Sets the connection status monitoring watcher to the specified watcher.
     * 
     * @param conf
     *            configuration to pull ensemble and other settings from
     * @param watcher
     *            watcher to monitor connection changes
     * @return connection to zookeeper
     * @throws IOException
     *             if unable to connect to zk or config problem
     */

    public static ZooKeeper conn(Conf conf, Watcher watcher) throws IOException {
        String ensemble = null;
        ensemble = conf.get("ZKServer");
        if (ensemble == null) {
            throw new IOException("Unable to determine ZooKeeper ensemble");
        }
        int timeout = conf.getInt("zookeeper.session.timeout", 180 * 1000);
        LOG.debug(" opening connection to ZooKeeper with ensemble (" + ensemble
                + "), timeout is" + timeout);
        ZooKeeper zk = new ZooKeeper(ensemble, timeout, watcher);
        // zk.addAuthInfo("digest", conf.get("auth").getBytes());
        return zk;
    }

    /**
     * Check if the specified node exists. Sets no watches.
     * 
     * Returns node version if node exists, -1 if not exists. Returns an
     * exception if there is an unexpected zookeeper exception.
     * 
     * @param zkw
     *            zk reference
     * @param znode
     *            path of node to watch
     * @return version of the node if it exists, -1 if does not exist
     * @throws KeeperException
     *             if unexpected zookeeper exception
     */
    public static int checkExists(ZooKeeperWatcher zkw, String znode)
            throws KeeperException {
        try {
            Stat s = zkw.getZooKeeper().exists(znode, null);
            return s != null ? s.getVersion() : -1;
        } catch (KeeperException e) {
            LOG.warn(zkw.prefix("Unable to set watcher on znode (" + znode
                    + ")"), e);
            zkw.keeperException(e);
            return -1;
        } catch (InterruptedException e) {
            LOG.warn(zkw.prefix("Unable to set watcher on znode (" + znode
                    + ")"), e);
            zkw.interruptedException(e);
            return -1;
        }
    }

    /**
     * checkExists的重试版本
     * 
     * @param zkw
     * @param znode
     * @return
     * @throws KeeperException
     */
    public static int checkExistsRetry(ZooKeeperWatcher zkw, String znode)
            throws KeeperException {
        return checkExists(zkw, znode);
    }

    /**
     * Creates the specified node, if the node does not exist. Does not set a
     * watch and fails silently if the node already exists.
     * 
     * The node created is persistent and open access. fixed by yilai,加返回值
     * 
     * @param zkw
     *            zk reference
     * @param znode
     *            path of node
     * @return true:建立 false:已经存在
     * @throws KeeperException
     *             if unexpected zookeeper exception
     */
    public static boolean creatAndFailSilent(ZooKeeperWatcher zkw, String znode)
            throws KeeperException {
        try {
            ZooKeeper zk = zkw.getZooKeeper();
            if (zk.exists(znode, false) == null) {
                zk.create(znode, new byte[0], Ids.OPEN_ACL_UNSAFE,
                        CreateMode.PERSISTENT);
                return true;
            }
        } catch (KeeperException.NodeExistsException nee) {
        } catch (InterruptedException ie) {
            zkw.interruptedException(ie);
        }
        return false;
    }

    /**
     * Creates the specified node, if the node does not exist. Does not set a
     * watch and fails silently if the node already exists.
     * 
     * The node created is persistent and open access. fixed by yilai,加返回值
     * 
     * @param zkw
     *            zk reference
     * @param znode
     *            path of node
     * @param data
     *            :写入的数据
     * @return true:建立 false:已经存在
     * @throws KeeperException
     *             if unexpected zookeeper exception
     */
    public static boolean creatAndFailSilent(ZooKeeperWatcher zkw,
            String znode, byte[] data) throws KeeperException {
        try {
            ZooKeeper zk = zkw.getZooKeeper();
            if (zk.exists(znode, false) == null) {
                zk.create(znode, data, Ids.OPEN_ACL_UNSAFE,
                        CreateMode.PERSISTENT);
                return true;
            }
        } catch (KeeperException.NodeExistsException nee) {
        } catch (InterruptedException ie) {
            zkw.interruptedException(ie);
        }
        return false;
    }

    /**
     * creatAndFailSilentRetry 重试功能
     * 
     * @param zkw
     * @param znode
     * @return
     * @throws KeeperException
     */

    public static boolean creatAndFailSilentRetry(ZooKeeperWatcher zkw,
            String znode) throws KeeperException {
        return creatAndFailSilent(zkw, znode);
    }

    /**
     * Sets the data of the existing znode to be the specified data. Ensures
     * that the current data has the specified expected version.
     * 
     * <p>
     * If the node does not exist, a {@link NoNodeException} will be thrown.
     * 
     * <p>
     * If their is a version mismatch, method returns null.
     * 
     * <p>
     * No watches are set but setting data will trigger other watchers of this
     * node.
     * 
     * <p>
     * If there is another problem, a KeeperException will be thrown.
     * 
     * @param zkw
     *            zk reference
     * @param znode
     *            path of node
     * @param data
     *            data to set for node
     * @param expectedVersion
     *            version expected when setting data
     * @return true if data set, false if version mismatch
     * @throws KeeperException
     *             if unexpected zookeeper exception
     */
    public static boolean setData(ZooKeeperWatcher zkw, String znode,
            byte[] data, int expectedVersion) throws KeeperException,
            KeeperException.NoNodeException {
        try {
            LOG.info("zkserver set data, znode=\"" + znode + "\",data=\""
                    + ToolUtil.toStr(data) + "\"");
            return zkw.getZooKeeper().setData(znode, data, expectedVersion) != null;
        } catch (InterruptedException e) {
            zkw.interruptedException(e);
            return false;
        }
    }

    /**
     * Sets the data of the existing znode to be the specified data. The node
     * must exist but no checks are done on the existing data or version.
     * 
     * <p>
     * If the node does not exist, a {@link NoNodeException} will be thrown.
     * 
     * <p>
     * No watches are set but setting data will trigger other watchers of this
     * node.
     * 
     * <p>
     * If there is another problem, a KeeperException will be thrown.
     * 
     * @param zkw
     *            zk reference
     * @param znode
     *            path of node
     * @param data
     *            data to set for node
     * @throws KeeperException
     *             if unexpected zookeeper exception
     */
    public static void setData(ZooKeeperWatcher zkw, String znode, byte[] data)
            throws KeeperException, KeeperException.NoNodeException {
        setData(zkw, znode, data, -1);
    }

    public static void setDataRetry(ZooKeeperWatcher zkw, String znode,
            byte[] data) throws KeeperException,
            KeeperException.NoNodeException {
        setData(zkw, znode, data, -1);
    }

    /**
     * Watch the specified znode for delete/create/change events. The watcher is
     * set whether or not the node exists. If the node already exists, the
     * method returns true. If the node does not exist, the method returns
     * false.
     * 
     * @param zkw
     *            zk reference
     * @param znode
     *            path of node to watch
     * @return true if znode exists, false if does not exist or error
     * @throws KeeperException
     *             if unexpected zookeeper exception
     */
    public static boolean watchAndCheckExists(ZooKeeperWatcher zkw, String znode)
            throws KeeperException {
        try {
            Stat s = zkw.getZooKeeper().exists(znode, zkw);
            LOG.debug(zkw.prefix("Set watcher on existing znode " + znode));
            return s != null ? true : false;
        } catch (KeeperException e) {
            LOG.warn(zkw.prefix("Unable to set watcher on znode " + znode), e);
            zkw.keeperException(e);
            return false;
        } catch (InterruptedException e) {
            LOG.warn(zkw.prefix("Unable to set watcher on znode " + znode), e);
            zkw.interruptedException(e);
            return false;
        }
    }

    /**
     * 
     * Set the specified znode to be an ephemeral node carrying the specified
     * data.
     * 
     * If the node is created successfully, a watcher is also set on the node.
     * 
     * If the node is not created successfully because it already exists, this
     * method will also set a watcher on the node.
     * 
     * If there is another problem, a KeeperException will be thrown.
     * 
     * @param zkw
     *            zk reference
     * @param znode
     *            path of node
     * @param data
     *            data of node
     * @return true if node created, false if not, watch set in both cases
     * @throws KeeperException
     *             if unexpected zookeeper exception
     */
    public static boolean creatEphemeralNodeNoWatch(ZooKeeperWatcher zkw,
            String znode, byte[] data) throws KeeperException {
        try {
            if (checkExists(zkw, znode) != -1) {
                ToolUtil.sleep(zkw.getZooKeeper().getSessionTimeout());
            }
            if (checkExists(zkw, znode) == -1) {
                zkw.getZooKeeper().create(znode, data, Ids.OPEN_ACL_UNSAFE,
                        CreateMode.EPHEMERAL);
                LOG.info("zkserver create Ephemeral node, znode=\"" + znode
                        + "\", data=\"" + ToolUtil.toStr(data) + "\"");
            }
            return true;
        } catch (InterruptedException e) {
            LOG.info("Interrupted", e);
            Thread.currentThread().interrupt();
        }
        return false;
    }

    /**
     * 
     * Set the specified znode to be an ephemeral node carrying the specified
     * data.
     * 
     * If the node is created successfully, a watcher is also set on the node.
     * 
     * If the node is not created successfully because it already exists, this
     * method will also set a watcher on the node.
     * 
     * If there is another problem, a KeeperException will be thrown.
     * 
     * @param zkw
     *            zk reference
     * @param znode
     *            path of node
     * @param data
     *            data of node
     * @return true if node created, false if not, watch set in both cases
     * @throws KeeperException
     *             if unexpected zookeeper exception
     */
    public static boolean creatEphemeralNodeAndWatch(ZooKeeperWatcher zkw,
            String znode, byte[] data) throws KeeperException {
        try {
            LOG.info("zkserver create Ephemeral Node, znode=\"" + znode
                    + "\", data=\"" + ToolUtil.toStr(data) + "\"");
            zkw.getZooKeeper().create(znode, data, Ids.OPEN_ACL_UNSAFE,
                    CreateMode.EPHEMERAL);
            // fixed by yilai,add watcher if node is new
            watchAndCheckExists(zkw, znode);
        } catch (KeeperException.NodeExistsException nee) {
            if (!watchAndCheckExists(zkw, znode)) {
                // It did exist but now it doesn't, try again
                return creatEphemeralNodeAndWatch(zkw, znode, data);
            }
            return false;
        } catch (InterruptedException e) {
            LOG.info("Interrupted", e);
            Thread.currentThread().interrupt();
        }
        return true;
    }

    /**
     * Lists the children znodes of the specified znode. Also sets a watch on
     * the specified znode which will capture a NodeDeleted event on the
     * specified znode as well as NodeChildrenChanged if any children of the
     * specified znode are created or deleted.
     * 
     * Returns null if the specified node does not exist. Otherwise returns a
     * list of children of the specified node. If the node exists but it has no
     * children, an empty list will be returned.
     * 
     * @param zkw
     *            zk reference
     * @param znode
     *            path of node to list and watch children of
     * @return list of children of the specified node, an empty list if the node
     *         exists but has no children, and null if the node does not exist
     * @throws KeeperException
     *             if unexpected zookeeper exception
     */
    public static List<String> lsChildrenAndWatchForNewChildren(
            ZooKeeperWatcher zkw, String znode) throws KeeperException {
        try {
            // LOG.info(".......znode="+znode);
            List<String> children = zkw.getZooKeeper().getChildren(znode, zkw);
            return children;
        } catch (KeeperException.NoNodeException ke) {
            LOG.debug(zkw.prefix("Unable to list children of znode " + znode
                    + " because node does not exist (not an error)"));
            return null;
        } catch (KeeperException e) {
            LOG.warn(
                    zkw.prefix("Unable to list children of znode " + znode
                            + " "), e);
            zkw.keeperException(e);
            return null;
        } catch (InterruptedException e) {
            LOG.warn(
                    zkw.prefix("Unable to list children of znode " + znode
                            + " "), e);
            zkw.interruptedException(e);
            return null;
        }
    }

    /**
     * List all the children of the specified znode, setting a watch for
     * children changes and also setting a watch on every individual child in
     * order to get the NodeCreated and NodeDeleted events.
     * 
     * @param zkw
     *            zookeeper reference
     * @param znode
     *            node to get children of and watch
     * @return list of znode names, null if the node doesn't exist
     * @throws KeeperException
     */
    public static List<String> lsChildrenAndWatchThem(ZooKeeperWatcher zkw,

    String znode) throws KeeperException {
        List<String> children = lsChildrenAndWatchForNewChildren(zkw, znode);
        if (children == null) {
            return null;
        }
        for (String child : children) {
            watchAndCheckExists(zkw, joinZNode(znode, child));
        }
        return children;
    }

    /**
     * Lists the children of the specified znode without setting any watches.
     * 
     * Used to list the currently online regionservers and their addresses.
     * 
     * Sets no watches at all, this method is best effort.
     * 
     * Returns an empty list if the node has no children. Returns null if the
     * parent node itself does not exist.
     * 
     * @param zkw
     *            zookeeper reference
     * @param znode
     *            node to get children of as addresses
     * @return list of data of children of specified znode, empty if no
     *         children, null if parent does not exist
     * @throws KeeperException
     *             if unexpected zookeeper exception
     */
    public static List<String> lsChildrenNoWatch(ZooKeeperWatcher zkw,
            String znode) throws KeeperException {
        List<String> children = null;
        try {
            // List the children without watching
            children = zkw.getZooKeeper().getChildren(znode, null);
        } catch (KeeperException.NoNodeException nne) {
            return null;
        } catch (InterruptedException ie) {
            zkw.interruptedException(ie);
        }
        return children;
    }

    /**
     * Delete all the children of the specified node but not the node itself.
     * 
     * Sets no watches. Throws all exceptions besides dealing with deletion of
     * children.
     */
    public static void delChildrenRecursively(ZooKeeperWatcher zkw, String node)
            throws KeeperException {
        List<String> children = ZKUtil.lsChildrenNoWatch(zkw, node);
        if (children == null || children.isEmpty())
            return;
        for (String child : children) {
            if (child.equals("pingstatus"))
                continue;
            delNodeRecursively(zkw, joinZNode(node, child));
        }
    }

    /**
     * Delete the specified node and all of it's children.
     * 
     * Sets no watches. Throws all exceptions besides dealing with deletion of
     * children.
     */
    public static void delNodeRecursively(ZooKeeperWatcher zkw, String node)
            throws KeeperException {
        try {
            List<String> children = ZKUtil.lsChildrenNoWatch(zkw, node);
            if (!children.isEmpty()) {
                for (String child : children) {
                    delNodeRecursively(zkw, joinZNode(node, child));
                }
            }
            zkw.getZooKeeper().delete(node, -1);
        } catch (InterruptedException ie) {
            zkw.interruptedException(ie);
        }
    }

    public static String joinZNode(String prefix, String suffix) {
        // return prefix + ZNODE_PATH_SEPARATOR + suffix;
        if (prefix.equals("/")) {
            return new StringBuilder().append(ZNODE_PATH_SEPARATOR)
                    .append(suffix).toString();
        } else {
            return new StringBuilder().append(prefix)
                    .append(ZNODE_PATH_SEPARATOR).append(suffix).toString();
        }
    }

    /**
     * 建立顺序节点
     * 
     * @param zkw
     * @param znode
     * @param data
     * @return 真实的znode的值
     * @throws KeeperException
     */
    public static String creatSeqNode(ZooKeeperWatcher zkw, String znode,
            byte[] data) throws KeeperException {
        try {
            LOG.info("zkserver create data, znode=\"" + znode + "\",data=\""
                    + ToolUtil.toStr(data) + "\"");
            String ret = zkw.getZooKeeper().create(znode, data,
                    Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT_SEQUENTIAL);
            return ret;
        } catch (InterruptedException e) {
            zkw.interruptedException(e);
        }
        return null;
    }

    /**
     * Get the data at the specified znode and set a watch.
     * 
     * Returns the data and sets a watch if the node exists. Returns null and no
     * watch is set if the node does not exist or there is an exception.
     * 
     * @param zkw
     *            zk reference
     * @param znode
     *            path of node
     * @return data of the specified znode, or null
     * @throws KeeperException
     *             if unexpected zookeeper exception
     */
    public static byte[] getDataAndWatch(ZooKeeperWatcher zkw, String znode)
            throws KeeperException {
        try {
            byte[] data = zkw.getZooKeeper().getData(znode, zkw, null);
            // fixecd by yilai ,下面行注释了
            // logRetrievedMsg(zkw, znode, data, true);
            return data;
        } catch (KeeperException.NoNodeException e) {
            LOG.debug(zkw.prefix("Unable to get data of znode " + znode
                    + " because node does not exist (not an error)"));
            return null;
        } catch (KeeperException e) {
            LOG.warn(zkw.prefix("Unable to get data of znode " + znode), e);
            zkw.keeperException(e);
            return null;
        } catch (InterruptedException e) {
            LOG.warn(zkw.prefix("Unable to get data of znode " + znode), e);
            zkw.interruptedException(e);
            return null;
        }
    }

    /**
     * Get the data at the specified znode and set a watch.
     * 
     * Returns the data and sets a watch if the node exists. Returns null and no
     * watch is set if the node does not exist or there is an exception.
     * 
     * @param zkw
     *            zk reference
     * @param znode
     *            path of node
     * @return data of the specified znode, or null
     * @throws KeeperException
     *             if unexpected zookeeper exception
     */
    public static byte[] getDataNoWatch(ZooKeeperWatcher zkw, String znode)
            throws KeeperException {
        try {
            byte[] data = zkw.getZooKeeper().getData(znode, null, null);
            // Fixed by yilai ,下面行注释了
            // logRetrievedMsg(zkw, znode, data, true);
            return data;
        } catch (KeeperException.NoNodeException e) {
            LOG.debug(zkw.prefix("Unable to get data of znode " + znode
                    + " because node does not exist (not an error)"));
            return null;
        } catch (KeeperException e) {
            LOG.warn(zkw.prefix("Unable to get data of znode " + znode), e);
            zkw.keeperException(e);
            return null;
        } catch (InterruptedException e) {
            LOG.warn(zkw.prefix("Unable to get data of znode " + znode), e);
            zkw.interruptedException(e);
            return null;
        }
    }

    /**
     * Atomically add watches and read data from all unwatched unassigned nodes.
     * 
     * <p>
     * This works because master is the only person deleting nodes.
     */
    public static List<NodeAndData> watchAndGetNewChildren(
            ZooKeeperWatcher zkw, String baseNode) throws KeeperException {
        List<NodeAndData> newNodes = new ArrayList<NodeAndData>();
        synchronized (zkw.getNodes()) {
            List<String> nodes = lsChildrenAndWatchForNewChildren(zkw, baseNode);
            for (String node : nodes) {
                String nodePath = joinZNode(baseNode, node);
                if (!zkw.getNodes().contains(nodePath)) {
                    byte[] data = getDataAndWatch(zkw, nodePath);
                    newNodes.add(new NodeAndData(nodePath, data));
                    zkw.getNodes().add(nodePath);
                }
            }
        }
        return newNodes;
    }

    /**
     * 清除zkw内部的watcher nodes集合的某个node
     * 和watchAndGetNewChildren配合使用，以便再次添加相同节点能收到通知
     * 
     * @param zkw
     * @param baseNode
     */
    public static void delNodeFromInsideSet(ZooKeeperWatcher zkw,
            String baseNode) {
        synchronized (zkw.getNodes()) {
            zkw.getNodes().remove(baseNode);
        }
    }

    /**
     * Simple class to hold a node path and node data.
     */
    public static class NodeAndData {
        private String node;
        private byte[] data;

        public NodeAndData(String node, byte[] data) {
            this.node = node;
            this.data = data;
        }

        public String getNode() {
            return node;
        }

        public byte[] getData() {
            return data;
        }

        @Override
        public String toString() {
            return node + " (" + ToolUtil.toStr(data) + ")";
        }
    }

    /**
     * 得到节点的状态
     * 
     * @param zkw
     * @param znode
     * @return
     * @throws KeeperException
     */
    public static Stat getStatNoWatch(ZooKeeperWatcher zkw, String znode)
            throws KeeperException {
        try {
            Stat stat = zkw.getZooKeeper().exists(znode, null);
            return stat;
        } catch (KeeperException e) {
            LOG.error(zkw.prefix("Unable to get znode " + znode + " "), e);
            zkw.keeperException(e);
            return null;
        } catch (InterruptedException e) {
            LOG.warn(zkw.prefix("Unable to get znode " + znode + " "), e);
            zkw.interruptedException(e);
            return null;
        }
    }

    /**
     * 得到相应IP机器远程调用命令的路径
     * 
     * @return
     */
    public static String getIpPath(String ip) {
        return joinZNode(Constant.ROOT + Constant.REMOTECONTROL, ip);
    }

    /**
     * 得到input的路径
     * 
     * @return
     */
    public static String getInputPath(String ip) {
        return joinZNode(Constant.ROOT + Constant.REMOTECONTROL, ip
                + Constant.INPUT);
    }

    /**
     * 得到output的路径
     * 
     * @return
     */
    public static String getOutputPath(String ip) {
        return joinZNode(Constant.ROOT + Constant.REMOTECONTROL, ip
                + Constant.OUTPUT);
    }

    /**
     * 得到outputResult的路径
     * 
     * @return
     */
    public static String getOutputRetPath(String ip, long serialID) {
        return joinZNode(Constant.ROOT + Constant.REMOTECONTROL, ip
                + Constant.OUTPUT + "/cmd@" + serialID);
    }

    /**
     * Returns the full path of the immediate parent of the specified node.
     * 
     * @param node
     *            path to get parent of
     * @return parent of path, null if passed the root node or an invalid node
     */
    public static String getParent(String node) {
        int idx = node.lastIndexOf(ZNODE_PATH_SEPARATOR);
        return idx <= 0 ? null : node.substring(0, idx);
    }
}