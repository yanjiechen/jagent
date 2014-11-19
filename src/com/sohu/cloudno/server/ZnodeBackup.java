package com.sohu.cloudno.server;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.util.List;

import org.apache.log4j.Logger;
import org.apache.zookeeper.KeeperException;

import com.sohu.cloudno.comm.Conf;
import com.sohu.cloudno.comm.ToolUtil;
import com.sohu.cloudno.comm.ZKUtil;
import com.sohu.cloudno.comm.ZooKeeperWatcher;

public class ZnodeBackup {
    private static final Logger LOG = Logger.getLogger(ZnodeBackup.class);

    private static void getChildZnode(PrintWriter pw, ZooKeeperWatcher zkw,
            String path) throws KeeperException {
        List<String> children = ZKUtil.lsChildrenNoWatch(zkw, path);
        if (!children.isEmpty()) {
            pw.write(path + "#"
                    + ToolUtil.toStr(ZKUtil.getDataNoWatch(zkw, path)) + "\n");
            for (String child : children) {
                getChildZnode(pw, zkw, ZKUtil.joinZNode(path, child));
            }
        }
    }

    private static void znodeExport() throws Exception {
        Conf conf = new Conf();
        conf.load("conf/ja.conf");
        ZooKeeperWatcher zkw = new ZooKeeperWatcher(conf, "JAgent", null);
        PrintWriter pw = new PrintWriter(new OutputStreamWriter(
                new FileOutputStream("zktree")), true);
        getChildZnode(pw, zkw, "/");
        pw.close();
    }

    private static void znodeImport() throws Exception {
        Conf conf = new Conf();
        conf.load("conf/ja.conf");
        ZooKeeperWatcher zkw = new ZooKeeperWatcher(conf, "JAgent", null);
        BufferedReader br = new BufferedReader(new InputStreamReader(
                new FileInputStream("zktree")));
        String line;
        while (null != (line = br.readLine())) {
            String[] arr = line.split("#");
            String path = line.split("#")[0];
            String value;
            if (1 == arr.length) {
                value = "";
            } else {
                value = line.split("#")[1];
            }
            if (!ZKUtil.creatAndFailSilent(zkw, path, ToolUtil.toByte(value))) {
                LOG.error("create " + line + " failed!");
            }
        }
        br.close();
    }

    public static void main(String[] args) {
        try {
            if (args.length == 1 && args[0].equals("export")) {
                znodeExport();
            } else if (args.length == 1 && args[0].equals("import")) {
                znodeImport();
            } else {
                System.out
                        .println("ZnodeBackup usage is ./znodebackup.sh {export|import}");
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}