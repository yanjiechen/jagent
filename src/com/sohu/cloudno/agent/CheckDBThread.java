package com.sohu.cloudno.agent;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.log4j.Logger;
import org.apache.zookeeper.KeeperException.ConnectionLossException;

import com.sohu.cloudno.comm.Conf;
import com.sohu.cloudno.comm.Constant;
import com.sohu.cloudno.comm.ToolUtil;
import com.sohu.cloudno.comm.ZKUtil;

public class CheckDBThread extends BaseThread {
    private static final Logger LOG = Logger.getLogger(CheckDBThread.class);
    private long SCRIPT_RUN_INTERVAL;
    private long MONITOR_INTERVAL_TIME;
    private volatile boolean isReset = false;

    CheckDBThread(JAgent ja, Conf conf, String desc) {
        super(ja, conf, desc);
        SCRIPT_RUN_INTERVAL = Long.parseLong(conf.get("script.run.interval"));
        MONITOR_INTERVAL_TIME = Long.parseLong(conf
                .get("monitor_interval_time"));
    }

    public void reset() {
        isReset = true;
    }

    /**
     * JA监控DB子线程逻辑
     * 
     */
    @Override
    public void run() {
        setThreadName();
        String root = Constant.ROOT + Constant.INSTANCE;
        List<String> grpList = new ArrayList<String>();
        Map<String, byte[]> zkTree = new LinkedHashMap<String, byte[]>();
        Map<String, byte[]> tmpTree = new LinkedHashMap<String, byte[]>();
        long refreshTime = 3600000 * (12 + Math.round(Math.random() * 12));
        long time = 0;
        String infs_raw = "";

        for (;; ToolUtil.sleep(MONITOR_INTERVAL_TIME)) {
            // LOG.info("CheckDBThread");
            if (isReset) {
                isReset = false;
                grpList.clear();
                zkTree.clear();
                ToolUtil.sleep(ja.getZkw().getZooKeeper().getSessionTimeout());
            }

            // 定期跳过本地镜像同步zk节点
            time += MONITOR_INTERVAL_TIME;
            if (time >= refreshTime
                    && infs_raw.equals(ToolUtil.runOSCmdRaw(
                            Constant.SH_CHECK_DB, SCRIPT_RUN_INTERVAL))) {
                grpList.clear();
                zkTree.clear();
                time = 0;
                refreshTime = 3600000 * (12 + Math.round(Math.random() * 12));
            }

            // 读取本地数据库实例列表信息
            infs_raw = ToolUtil.runOSCmdRaw(Constant.SH_CHECK_DB,
                    SCRIPT_RUN_INTERVAL);
            if (null == infs_raw || infs_raw.equals("")) {
                infs_raw = "";
                continue;
            }
            String infs = infs_raw.replace("JA_RESULT=", "").trim();
            if (infs.equals("E")) {
                LOG.error("script error!");
                continue;
            }
            try {
                if (infs.equals("Y") || infs.equals("R")) {
                    for (Iterator<Entry<String, byte[]>> i = zkTree.entrySet()
                            .iterator(); i.hasNext();) {
                        String inst = i.next().getKey();
                        ZKUtil.delNodeRecursively(ja.getZkw(), inst);
                        i.remove();
                        zkTree.remove(inst);
                    }
                    LOG.info("No db running now, try again later...");
                    continue;
                }

                tmpTree.clear();
                for (String inf : infs.split("\n")) {
                    String[] dbInf = inf.split(":");
                    if (6 != dbInf.length)
                        continue;
                    String GRP = ZKUtil.joinZNode(root, dbInf[2]);
                    String grp = ZKUtil.joinZNode(GRP, "grp@" + dbInf[3]);
                    String inst = ZKUtil.joinZNode(grp, "inst@" + dbInf[4]);
                    StringBuilder data = new StringBuilder(dbInf[5]);

                    if (!dbInf[0].equals("C")) {
                        data.append("@is_ha=").append(dbInf[0])
                                .append("@is_keepalived=").append(dbInf[0]);
                    }

                    if (dbInf[0].equals("Y")
                            && dbInf[1].equalsIgnoreCase("mysql")) {
                        String haconf = ToolUtil.runOSCmdRaw(
                                Constant.SH_HACONF, SCRIPT_RUN_INTERVAL);
                        if (null != haconf && haconf.trim().equals("10")) {
                            data.append("@haVersion=keepalived+lvs");
                        } else {
                            data.append("@haVersion=keepalived");
                        }
                    }

                    tmpTree.put(inst, ToolUtil.toByte(data.toString()));

                    if (!grpList.contains(GRP)) {
                        if (ZKUtil.creatAndFailSilent(ja.getZkw(), GRP,
                                ToolUtil.toByte("GRP")))
                            grpList.add(GRP);
                    }

                    // zookeeper的watcher机制问题，两次动作最好错开一定时间，要不消息会丢囧rz
                    ToolUtil.sleep(200);

                    if (!grpList.contains(grp)) {
                        if (dbInf[1].equalsIgnoreCase("mysql")
                                || dbInf[1].equalsIgnoreCase("oracle")
                                || dbInf[1].equalsIgnoreCase("mongodb")
                                || dbInf[1].equalsIgnoreCase("PXCW")) {
                            if (ZKUtil.creatAndFailSilent(ja.getZkw(), grp,
                                    ToolUtil.toByte("W")))
                                grpList.add(grp);
                        } else if (dbInf[1].equalsIgnoreCase("lvs")
                                || dbInf[1].equalsIgnoreCase("proxy")
                                || dbInf[1].equalsIgnoreCase("PXCR")) {
                            if (ZKUtil.creatAndFailSilent(ja.getZkw(), grp,
                                    ToolUtil.toByte("R")))
                                grpList.add(grp);
                        }
                    }
                }

                // delete zkTree和ZooKeeper的多余节点
                for (Iterator<Entry<String, byte[]>> i = zkTree.entrySet()
                        .iterator(); i.hasNext();) {
                    ToolUtil.sleep(200);
                    String inst = i.next().getKey();

                    if (!tmpTree.containsKey(inst)) {
                        ZKUtil.delNodeRecursively(ja.getZkw(), inst);
                        i.remove();
                        zkTree.remove(inst);
                    }
                }

                // update insert zkTree和ZooKeeper的节点
                for (Entry<String, byte[]> node : tmpTree.entrySet()) {
                    ToolUtil.sleep(200);
                    String inst = node.getKey();
                    byte[] data = node.getValue();

                    if (zkTree.containsKey(inst)) {
                        String zkDataStr = ToolUtil.toStr(zkTree.get(inst));
                        String tmpDataStr = ToolUtil.toStr(data);

                        if (!zkDataStr.equals(tmpDataStr)) {
                            if (ZKUtil.setData(ja.getZkw(), inst, data, -1))
                                zkTree.put(inst, data);
                        }
                    } else {
                        if (ZKUtil.creatEphemeralNodeNoWatch(ja.getZkw(), inst,
                                data))
                            zkTree.put(inst, data);
                    }
                }
            } catch (ConnectionLossException e) {
            } catch (Exception e) {
                abort(desc, e);
            }
        }
    }
}