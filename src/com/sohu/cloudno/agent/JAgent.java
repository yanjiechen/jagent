package com.sohu.cloudno.agent;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import org.apache.log4j.Logger;
import org.apache.zookeeper.KeeperException;

import com.sohu.cloudno.comm.Abortable;
import com.sohu.cloudno.comm.Conf;
import com.sohu.cloudno.comm.ToolUtil;
import com.sohu.cloudno.comm.ZKUtil;
import com.sohu.cloudno.comm.ZooKeeperWatcher;

/**
 * JAgent的主线程，监控本机的资源情况并同步到zk
 * 
 * @author yanjiechen
 * @version 0.3.4
 * 
 */
public class JAgent implements Abortable {
    private static final Logger LOG = Logger.getLogger(JAgent.class);
    // 配置信息
    private Conf conf = null;
    // JAgent实例对象
    private JAgent ja = null;
    // 连接zk的实例
    private ZooKeeperWatcher zkw = null;
    // 检查Lvs状态的线程
    private CheckDBThread checkDBThread = null;
    // 远程调用命令节点路径
    private String inputPath = null;
    // 子线程异常信息汇总，用于主线程处理使用
    private List<String> threadThrowable = null;

    JAgent() {
        conf = new Conf();
        threadThrowable = new CopyOnWriteArrayList<String>();
    }

    public synchronized ZooKeeperWatcher getZkw() {
        return zkw;
    }

    public Conf getConf() {
        return conf;
    }

    /**
     * 系统初始化，完成下列工作 1.读本地配置文件启动JAgent 2.连接zk 3.注册远程控制监听器 4.启动监控数据库子线程
     * 
     * @param JAgent
     * @return 成功返回true，否则false
     * @throws Exception
     * 
     */
    private boolean init(JAgent ja, String[] args) throws Exception {
        this.ja = ja;

        // 设置名字，便于看调试的输出
        Thread.currentThread().setName("JAgent" + Math.round(Math.random() * 10000));
        // 加载配置文件
        conf.load();
        // 打印版本
        if ((args.length == 1) && (args[0].equals("version"))) {
            System.out.println("JAgent version " + conf.get("version"));
            return false;
        }

        // 从本地文件得到zkServer
        if (null == conf.get("ZKServer")) {
            LOG.error("ZK server is null");
            return false;
        }

        zkw = new ZooKeeperWatcher(conf, "JAgent", ja);
        LOG.debug("zkw is=" + zkw);
        LOG.debug("JAgentInit connect zkserver sucessfully");

        // 初始化zk控制树信息
        String ip = ToolUtil.getPhysicalIp();
        ZKUtil.creatAndFailSilent(zkw, ZKUtil.getIpPath(ip));

        inputPath = ZKUtil.getInputPath(ip);
        ZKUtil.creatAndFailSilent(zkw, inputPath);
        ZKUtil.delChildrenRecursively(zkw, inputPath);

        ZKUtil.creatAndFailSilent(zkw, ZKUtil.getOutputPath(ip));

        registerInputListener();

        // 启动监控数据库子线程
        checkDBThread = new CheckDBThread(ja, conf, "CheckDBThread");
        checkDBThread.start();

        return true;
    }

    private void run() {
        LOG.debug("JAgent is running....");
        long MONITOR_RETRY_TIMES = Long.parseLong(conf
                .get("monitor_retry_times"));
        for (;; ToolUtil.sleep(MONITOR_RETRY_TIMES)) {
            // LOG.info("Thread num: " + Thread.activeCount());
            if (threadThrowable.isEmpty())
                continue;

            if (threadThrowable.get(0).equals("SessionExpiredException")) {
                try {
                    // 再进行一次尝试，不行就catch
                    ZKUtil.checkExists(zkw, "/");
                } catch (Exception e) {
                    // 重建zkw对象、注册监听器、重置checkDBThread
                    try {
                        if (zkw != null)
                            zkw.close();

                        zkw = new ZooKeeperWatcher(conf, "JAgent", ja);
                        registerInputListener();
                        checkDBThread.reset();
                        LOG.info("CheckDBThread" + checkDBThread.getId()
                                + " reset");
                    } catch (Exception e1) {
                    }
                }
            }
            threadThrowable.remove(0);
        }
    }

    /**
     * 其他线程有问题，标识thread停止
     * 
     * @param msg
     *            Why we're aborting.
     * @param e
     *            Throwable that caused abort. Can be null.
     */
    @Override
    public void abort(String msg, Throwable e) {
        // 出现ConnectionLossException直接无视掉
        if (!e.getClass().getSimpleName().equals("ConnectionLossException")) {
            threadThrowable.add(e.getClass().getSimpleName());
            LOG.fatal(msg, e);
            LOG.error(ToolUtil.exceptionMsg(e));
        }
    }

    /**
     * 注册/v0002/remotecontrol/ip/input节点的监听器
     * 
     */
    private void registerInputListener() throws KeeperException {
        zkw.registerListener(new RemoteCommandHandle(ja));
        ZKUtil.watchAndGetNewChildren(zkw, inputPath);
    }

    public static void main(String[] args) {
        JAgent ja = new JAgent();

        try {
            if (ja.init(ja, args))
                ja.run();
        } catch (Exception e) {
            LOG.error("program encount exception", e);
        } finally {
            try {
                if (ja.zkw != null)
                    ja.zkw.close();
                ja.checkDBThread.interrupt();
            } catch (Exception e) {
            }
            System.exit(0);
        }
    }
}