package com.sohu.cloudno.test;

import java.util.List;

import org.apache.zookeeper.KeeperException;

import com.sohu.cloudno.comm.ZKUtil;
import com.sohu.cloudno.comm.ZKUtil.NodeAndData;
import com.sohu.cloudno.comm.ZooKeeperListener;
import com.sohu.cloudno.comm.ZooKeeperWatcher;

public class Listener extends ZooKeeperListener {

    Listener(ZooKeeperWatcher zkw) {
        super(zkw);
    }

    public void nodeDeleted(String path) {
        System.out.println("delete" + path);
    }

    public void nodeDataChanged(String path) {
        System.out.println("change" + path);
    }

    public void nodeChildrenChanged(String path) {
        try {
            List<NodeAndData> list = ZKUtil.watchAndGetNewChildren(zkw, path);
            for (NodeAndData node : list) {
                System.out.println("child" + node.getNode());
                ZKUtil.watchAndGetNewChildren(zkw, node.getNode());
            }
        } catch (KeeperException e) {
            e.printStackTrace();
        }
    }
}