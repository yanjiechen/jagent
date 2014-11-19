package com.sohu.cloudno.server;

import java.util.Date;
import java.util.List;

import org.apache.log4j.Logger;
import org.apache.zookeeper.data.Stat;

import com.sohu.cloudno.comm.Constant;
import com.sohu.cloudno.comm.ToolUtil;
import com.sohu.cloudno.comm.ZKUtil;
import com.sohu.cloudno.comm.ZKUtil.NodeAndData;
import com.sohu.cloudno.comm.ZooKeeperListener;
import com.sohu.cloudno.comm.ZooKeeperWatcher;

/**
 * 通过检查instance group是否已经建立好了，通知主线程可以开始工作
 * 
 * @author yanjiechen
 * 
 */
public class ZnodeEnqueueListener extends ZooKeeperListener {
    private static final Logger LOG = Logger.getLogger(ZnodeEnqueueListener.class);
    private ZnodeEnqueue zn;

    ZnodeEnqueueListener(ZooKeeperWatcher zkw, ZnodeEnqueue zn) {
        super(zkw);
        this.zn = zn;
    }

    /**
     * Called when a node has been deleted
     * 
     * @param path
     *            full path of the deleted node
     */
    @Override
    public void nodeDeleted(String path) {
        try {
            // 清除节点
            ZKUtil.delNodeFromInsideSet(zkw, path);

            // 入队
            String value = "0##" + path + "##1##" + new Date().getTime();
            if (path.indexOf("pingstatus") != -1) {
                LOG.info("insert into pingqueue, status is DELETE, path is \"" + path + "\"");
                ZKUtil.creatSeqNode(zkw, getQueuePath() + "/q@", ToolUtil.toByte(value));
            } else {
                LOG.info("insert into jaqueue, status is DELETE, path is \"" + path + "\"");
                ZKUtil.creatSeqNode(zkw, getJAQueuePath() + "/q@", ToolUtil.toByte(value));
            }
        } catch (Exception e) {
            LOG.error("error", e);
            // 通知主线程退出
            zn.abort("access zk server error", e);
        }
    }

    /**
     * 处理节点数据改变
     * 
     * @param path
     *            full path of the updated node
     */
    @Override
    public void nodeDataChanged(String path) {
        try {
            
            // 再次设置watcher,得到数据
            String data = ToolUtil.toStr(ZKUtil.getDataAndWatch(zkw, path));
            Stat stat = ZKUtil.getStatNoWatch(zkw, path);
            // 取当前Znode的父节点
            Stat statParent = ZKUtil.getStatNoWatch(zkw, ZKUtil.getParent(path));

            // 入队
            String value = "0#" + stat.getCzxid() + "#" + path + "#"
                          + statParent.getCzxid() + "#2#" + data + "#"
                          + stat.getMtime();
            
            if (path.indexOf("pingstatus") != -1) {
                LOG.info("insert into pingqueue, status is UPDATE, path is \"" + path + "\"");
                ZKUtil.creatSeqNode(zkw, getQueuePath() + "/q@",ToolUtil.toByte(value));
            } else {
                LOG.info("insert into jaqueue, status is UPDATE, path is \"" + path + "\"");
                ZKUtil.creatSeqNode(zkw, getJAQueuePath() + "/q@",ToolUtil.toByte(value));
            }
        } catch (Exception e) {
            LOG.error("error", e);
            // 通知主线程退出
            zn.abort("access zk server error", e);
        }
    }

    /**
     * 处理节点添加
     * 
     * @param path
     *            full path of the node whose children have changed
     */
    @Override
    public void nodeChildrenChanged(String path) {
        try {
            if (path.indexOf(getGroupRootPath()) == 0) {
                // 再次设置children类型 watcher ,检查是否有节点新增，如果有新增设置data类型 watcher
                List<NodeAndData> list = ZKUtil.watchAndGetNewChildren(zkw,    path);
                // 如果有节点新增，入队（如果是删除，list为null)
                for (int i = 0; i < list.size(); ++i) {
                    String node = list.get(i).getNode();
                    ZKUtil.watchAndGetNewChildren(zkw, node);
                    Stat stat = ZKUtil.getStatNoWatch(zkw, node);
                    // 取当前Znode的父节点
                    Stat statParent = ZKUtil.getStatNoWatch(zkw, ZKUtil.getParent(node));

                    String value = "0#" + stat.getCzxid() + "#" + node + "#"
                            + statParent.getCzxid() + "#3#"
                            + ToolUtil.toStr(list.get(i).getData()) + "#"
                            + stat.getMtime();
                    if (path.indexOf("pingstatus") != -1) {
                        LOG.info("insert into pingqueue, status is CREATE, path is \""    + node + "\"");
                        ZKUtil.creatSeqNode(zkw, getQueuePath() + "/q@", ToolUtil.toByte(value));
                    } else {
                        LOG.info("insert into jaqueue, status is CREATE, path is \"" + node + "\"");
                        ZKUtil.creatSeqNode(zkw, getJAQueuePath() + "/q@",    ToolUtil.toByte(value));
                    }
                }
            }
        } catch (Exception e) {
            LOG.error("error", e);
            // 通知主线程退出
            zn.abort("access zk server error", e);
        }
    }

    /**
     * 得到queue的路径
     * 
     * @return String
     */
    public String getQueuePath() {
        return Constant.ROOT + Constant.QUEUE;
    }

    /**
     * 得到jaqueue的路径
     * 
     * @return String
     */
    public String getJAQueuePath() {
        return Constant.ROOT + Constant.JA_QUEUE;
    }

    /**
     * 得到GroupRootPath的路径
     * 
     * @return String
     */
    public String getGroupRootPath() {
        return Constant.ROOT + Constant.INSTANCE;
    }
}