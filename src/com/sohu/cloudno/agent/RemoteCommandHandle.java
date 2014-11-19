package com.sohu.cloudno.agent;

import java.util.List;

import org.apache.zookeeper.KeeperException;

import com.sohu.cloudno.comm.ToolUtil;
import com.sohu.cloudno.comm.ZKUtil;
import com.sohu.cloudno.comm.ZKUtil.NodeAndData;
import com.sohu.cloudno.comm.ZooKeeperListener;

/**
 * 通过检查inputNode是否已有需要执行的命令，通知主线程可以开始运行相应的远程调用
 * 
 * @author yanjiechen
 * 
 */
public class RemoteCommandHandle extends ZooKeeperListener {
    private JAgent ja;

    RemoteCommandHandle(JAgent ja) {
        super(ja.getZkw());
        this.ja = ja;
    }

    /**
     * 处理节点添加
     * 
     * @param path
     *            full path of the node whose children have changed
     */
    public void nodeChildrenChanged(String path) {
        try {
            List<NodeAndData> list = ZKUtil.watchAndGetNewChildren(ja.getZkw(),path);
            while (!list.isEmpty()) {
                String znode = list.get(0).getNode();
                ZKUtil.delNodeRecursively(ja.getZkw(), znode);
                long id = Long.parseLong(znode.substring(znode.lastIndexOf("@") + 1));
                String cmd = ToolUtil.toStr(list.get(0).getData());
                new ExecCmdThread(ja, ja.getConf(), "ExecCmdThread", id, cmd).start();
                list = ZKUtil.watchAndGetNewChildren(ja.getZkw(), path);
            }
        } catch (KeeperException e) {
            ja.abort(RemoteCommandHandle.class.getSimpleName(), e);
        }
    }
}