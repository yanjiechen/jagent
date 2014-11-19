package com.sohu.cloudno.api;

import java.util.List;
import java.util.TreeMap;

import org.apache.log4j.Logger;
import org.apache.zookeeper.KeeperException;

import com.sohu.cloudno.comm.ToolUtil;
import com.sohu.cloudno.comm.ZKUtil;
import com.sohu.cloudno.comm.ZooKeeperWatcher;

public class CommandResult {
    private static final Logger LOG = Logger.getLogger(CommandResult.class);
    private ZooKeeperWatcher zkw;
    private String ip;
    private long serialID;
    private int timeout;
    private String outputRetPath;
    private TreeMap<Integer, String> treeMap = new TreeMap<Integer, String>();

    public CommandResult(ZooKeeperWatcher zkw, String ip, long serialID,int timeout) {
        this.zkw = zkw;
        this.ip = ip;
        this.serialID = serialID;
        this.timeout = timeout;
        this.outputRetPath = ZKUtil.getOutputRetPath(this.ip, this.serialID);
    }

    /**
     * 检查output上是否已有远程执行结果
     * 
     * @return int
     * @throws KeeperException
     */
    public int getStat() throws KeeperException {
        int stat = 0;
        int sleeptime = 1000;
        int time = 0;
        while (time < timeout) {
            if (ZKUtil.checkExists(zkw, outputRetPath + "_0") == -1) {
                stat = -1;
            } else {
                stat = 0;
                break;
            }
            ToolUtil.sleep(sleeptime);
            time += sleeptime;
        }
        return stat;
    }

    /**
     * 按行读取命令执行结果
     * 
     * @return String
     * @throws KeeperException
     */
    public String readLine() throws KeeperException {
        // 获取output节点上的记录
        while (treeMap.isEmpty()) {
            List<String> children = ZKUtil.lsChildrenNoWatch(zkw,ZKUtil.getOutputPath(ip));

            for (String child : children) {
                if (!child.startsWith("cmd@" + serialID + "_")){
                    continue;
                }                    
                int key = Integer.parseInt(child.substring(child.lastIndexOf("_") + 1));
                String outputRetPathSeq = outputRetPath + "_" + key;
                String data = ToolUtil.toStr(ZKUtil.getDataNoWatch(zkw,outputRetPathSeq));
                if (data == null){
                    continue;
                }                    
                treeMap.put(key, data);
                ZKUtil.delNodeRecursively(zkw, outputRetPathSeq);
            }
            ToolUtil.sleep(3);
        }

        // 读取一行记录并返回
        int key = treeMap.firstKey();
        String value = treeMap.get(key);

        treeMap.remove(key);
        if (value.equals("EOF")) {
            return null;
        }
        LOG.info("Command result: " + value);
        return value;
    }
}