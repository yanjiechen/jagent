package com.sohu.cloudno.server;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;

import org.apache.log4j.Logger;

import com.sohu.cloudno.comm.Abortable;
import com.sohu.cloudno.comm.Conf;
import com.sohu.cloudno.comm.Constant;
import com.sohu.cloudno.comm.ToolUtil;
import com.sohu.cloudno.comm.ZKUtil;
import com.sohu.cloudno.comm.ZKUtil.NodeAndData;
import com.sohu.cloudno.comm.ZooKeeperWatcher;
import com.sohu.cloudno.util.StackTrace;

/**
 * 监控/v0002/instances的变化并入队
 * 
 * @author yanjiechen
 * @version 0.3.4
 * 
 */

public class ZnodeEnqueue implements Abortable {
    private static final Logger LOG = Logger.getLogger(ZnodeEnqueue.class);
    private Conf conf;
    // 是否需要停止
    private boolean stopped = false;
    // 连接zk的实例
    private ZooKeeperWatcher zkw;
    // 入队心跳数据库连接
    private Connection conn;

    /**
     * 系统初始化，完成下列工作 1.通过读本地配置文件启动ZnodeEnqueue 2.连接zk 3.注册监听 4.检查queue是否存在，不存在建立
     * TODO:5.节点选举功能 6.监听/v0002/instances/的所有变化 7.清除队列 8.queue插入初始化成功消息
     * 
     * @param zn
     * @param args
     * @return boolean 初始化成功返回true,否则false
     * @throws Exception
     * 
     */
    private boolean init(ZnodeEnqueue zn, String[] args) throws Exception {
        // 设置名字，便于看调试的输出
        Thread.currentThread().setName("ZnodeEnqueue");
        // 加载配置文件
        this.conf = new Conf();
        conf.load();
        // 打印版本
        if ((args.length == 1) && (args[0].equals("version"))) {
            LOG.info("ZnodeEnqueue version " + conf.get("version"));
            return false;
        }

        // 从本地文件得到zkServer
        if ((conf.get("ZKServer") != null) == false) {
            LOG.error("ZK server is null");
            return false;
        }

        String qPath = Constant.ROOT + Constant.QUEUE;
        String jaqPath = Constant.ROOT + Constant.JA_QUEUE;
        String instPath = Constant.ROOT + Constant.INSTANCE;

        zkw = new ZooKeeperWatcher(conf, "ZnodeEnqueue", zn);
        LOG.debug("zkw is=" + zkw);
        // 检查queue是否存在，不存在建立
        ZKUtil.creatAndFailSilentRetry(zkw, qPath);

        // 注册监听"/v0002/instances"的所有变化
        zkw.registerListener(new ZnodeEnqueueListener(zkw, zn));
        List<NodeAndData> list = ZKUtil.watchAndGetNewChildren(zkw, instPath);
        for (NodeAndData node : list) {
            List<NodeAndData> list1 = ZKUtil.watchAndGetNewChildren(zkw,
                    node.getNode());
            for (NodeAndData node1 : list1) {
                ZKUtil.watchAndGetNewChildren(zkw, node1.getNode());
            }
        }

        // 清除队列
        ZKUtil.delChildrenRecursively(zkw, qPath);
        ZKUtil.delChildrenRecursively(zkw, jaqPath);

        // pingqueue插入初始化成功消息
        LOG.info("insert first element into pingqueue, key=\"" + qPath    + "/q@\" value=1");
        ZKUtil.creatSeqNode(zkw, qPath + "/q@", ToolUtil.toByte("1"));
        
        // jaqueue插入初始化成功消息
        LOG.info("insert first element into jaqueue, key=\"" + jaqPath    + "/q@\" value=1");
        ZKUtil.creatSeqNode(zkw, jaqPath + "/q@", ToolUtil.toByte("1"));
        
        return true;
    }

    /**
     * 其他线程有问题，标识main thread停止
     * 
     * @param why
     *            Why we're aborting.
     * @param e
     *            Throwable that caused abort. Can be null.
     */
    @Override
    public void abort(String msg, Throwable e) {
        if (e != null){
            LOG.fatal(msg, e);
        }else{
            LOG.fatal(msg);
        }            
        stopped = true;
    }

    /**
     * 主线程每隔一段时间检查下列情况 1.是否其他线程有异常，如果异常需要退出
     * 
     * @throws ClassNotFoundException
     * @throws SQLException
     * 
     */
    private void run() throws ClassNotFoundException, SQLException {
        // Check if we should stop every second.
        LOG.info("ZnodeEnqueue is running....");
        Class.forName("oracle.jdbc.driver.OracleDriver");
        PreparedStatement ps = null;
        this.conn = DriverManager.getConnection("jdbc:oracle:thin:@" + conf.get("db.host") + ":" + conf.get("db.port") + ":" + conf.get("db.name"),
                                                conf.get("db.user"), 
                                                conf.get("db.passwd"));

        for (; !this.stopped; ToolUtil.sleep(60000)) {            
            //this.conn.prepareStatement("update CLOUD_HEARTBEAT set enqueue_time=sysdate").execute();
            //this.conn.commit();
            //modify by kangzhanwang start
            try{
                conn.setAutoCommit(false);
                ps = conn.prepareStatement("update CLOUD_HEARTBEAT set enqueue_time=sysdate");
                ps.executeUpdate();
                conn.commit();
            }catch(Exception ex){
                LOG.info(StackTrace.getExceptionTrace(ex));    
            }finally{
                if(ps!=null){
                    ps.close();
                }
            }
            //modify by kangzhanwang end
        }
    }

    public static void main(String[] args) {
        ZnodeEnqueue zn = new ZnodeEnqueue();

        try {
            if (zn.init(zn, args)){
                zn.run();
            }                
        } catch (Exception e) {
            LOG.error("Error encount", e);
        } finally {
            try {
                if (zn.conn != null){
                    zn.conn.close();
                }
                zn.zkw.close();
            } catch (Exception e) {
            }
            System.exit(0);
        }
    }
}