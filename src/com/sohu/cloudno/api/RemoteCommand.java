package com.sohu.cloudno.api;

import java.io.IOException;

import org.apache.log4j.Logger;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.server.quorum.QuorumPeerConfig.ConfigException;

import com.sohu.cloudno.comm.Conf;
import com.sohu.cloudno.comm.Conf.ConfException;
import com.sohu.cloudno.comm.Constant;
import com.sohu.cloudno.comm.ToolUtil;
import com.sohu.cloudno.comm.ZKUtil;
import com.sohu.cloudno.comm.ZooKeeperConnectionException;
import com.sohu.cloudno.comm.ZooKeeperWatcher;

public class RemoteCommand {
    private static final Logger LOG = Logger.getLogger(RemoteCommand.class);
    private ZooKeeperWatcher zkw;
    private Conf conf;

    public RemoteCommand() throws ConfException, ZooKeeperConnectionException,IOException, InterruptedException {
        // 设置名字，便于看调试的输出
        Thread.currentThread().setName("RemoteCommand");

        // 从本地文件得到zkServer
        conf = new Conf();
        conf.load();

        if ((conf.get("ZKServer") != null) == false) {
            LOG.error("ZK server is null");
            System.exit(0);
        }

        zkw = new ZooKeeperWatcher(conf, "RemoteCommand", null);
        LOG.debug("zkw is=" + zkw);
    }

    /**
     * 远程执行命令
     * 
     * @param ip
     * @param cmd
     * @param timeout
     * @return CommandResult
     * @throws ConfigException
     * @throws KeeperException
     */
    public CommandResult run(String ip, String cmd, int timeout)
            throws ConfigException, KeeperException {
        //modify by kangzhanwang 2013-11-13 start
        //增加if的大括号
        if (!ToolUtil.checkIP(ip)){
            throw new ConfigException("Format of ip \"" + ip + 
                                      "\" is wrong. ip length is " + ip.length() + 
                                      ", maybe include blank space?");
        }            
        if (cmd == null){
            throw new ConfigException("cmd is null");
        }    
        //modify by kangzhanwang 2013-11-13 end
        
        // 插入队列
        String znode = ZKUtil.creatSeqNode(zkw, 
                                           ZKUtil.getInputPath(ip) + "/cmd@",
                                           ToolUtil.toByte(cmd + Constant.FIELD_SPLIT + timeout));
        // 获取命令的序列号
        long serialID = Long.parseLong(znode.substring(znode.lastIndexOf("@") + 1));
        LOG.info("Execute command: " + cmd + Constant.FIELD_SPLIT + timeout);
        
        // 创建结果集对象
        CommandResult ret = new CommandResult(zkw, ip, serialID, timeout);
        return ret;
    }

    public void close() {
        if (zkw != null) {
            zkw.close();
        }
    }
}