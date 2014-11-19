package com.sohu.cloudno.test;

import java.util.List;

import com.sohu.cloudno.comm.Conf;
import com.sohu.cloudno.comm.ZKUtil;
import com.sohu.cloudno.comm.ZooKeeperWatcher;

public class Test extends Thread {
    public static void main(String[] args) throws Exception {

        Conf conf = new Conf();
        conf.load("conf/ja.conf");
        ZooKeeperWatcher zkw = new ZooKeeperWatcher(conf, "JAgent", null);
        String node = "/v0002/remotecontrol/10.10.58.168/output";
        
        /*if (ZKUtil.checkExists(zkw, node) > -1) {
            String zkNodeStatus = new String(ZKUtil.getDataNoWatch(zkw, node));
            if(!zkNodeStatus.split(",")[1].equals("down")){
                //ZKUtil.setData(zkw, node, ToolUtil.toByte(result));
            }
        }*/
        
        
        List<String> children = ZKUtil.lsChildrenNoWatch(zkw, node);
        if (children == null || children.isEmpty())
            return;
        for (String child : children) {
            ZKUtil.delNodeRecursively(zkw, ZKUtil.joinZNode(node, child));
        }
        //System.out.println(ZKUtil.checkExists(zkw, "/hbase"));
    }
}