package com.sohu.cloudno.agent;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PipedInputStream;
import java.io.PipedOutputStream;

import org.apache.commons.exec.CommandLine;
import org.apache.commons.exec.DefaultExecuteResultHandler;
import org.apache.commons.exec.DefaultExecutor;
import org.apache.commons.exec.ExecuteWatchdog;
import org.apache.commons.exec.PumpStreamHandler;
import org.apache.log4j.Logger;

import com.sohu.cloudno.comm.Constant;
import com.sohu.cloudno.comm.Conf;
import com.sohu.cloudno.comm.ToolUtil;
import com.sohu.cloudno.comm.ZKUtil;

public class ExecCmdThread extends BaseThread {
    private static final Logger LOG = Logger.getLogger(ExecCmdThread.class);
    private long id;
    private String cmd;

    ExecCmdThread(JAgent ja, Conf conf, String desc, long id, String cmd) {
        super(ja, conf, desc);
        this.id = id;
        this.cmd = cmd;
    }

    /**
     * JA执行中控机发起的远程命令
     * 
     */
    @Override
    public void run() {
        setThreadName();
        LOG.info("Start command " + id + ": " + cmd);
        try {
            int split = cmd.lastIndexOf(Constant.FIELD_SPLIT);
            int timeout = Integer.parseInt(cmd.substring(split + 1));
            cmd = cmd.substring(0, split);
            String ip = ToolUtil.getPhysicalIp();
            String outputRetPath = ZKUtil.getOutputRetPath(ip, id);

            // 执行中控发出的相应命令或脚本
            PipedOutputStream stdout = new PipedOutputStream();
            PumpStreamHandler psh = new PumpStreamHandler(stdout);
            CommandLine cl = CommandLine.parse(cmd);
            DefaultExecutor exec = new DefaultExecutor();
            exec.setStreamHandler(psh);

            ExecuteWatchdog watchdog = null;
            if (0 < timeout) {
                watchdog = new ExecuteWatchdog(timeout);
                exec.setWatchdog(watchdog);
            }

            DefaultExecuteResultHandler derh = new DefaultExecuteResultHandler();
            exec.execute(cl, derh);

            BufferedReader br = new BufferedReader(new InputStreamReader(
                    new PipedInputStream(stdout)));

            // 按照执行结果逐行写入output节点
            int i = 0;
            try {
                String line = null;
                while (null != (line = br.readLine())) {
                    LOG.info("Return command result: " + id + "_" + i);
                    String outputRetPathSeq = outputRetPath + "_" + i++;
                    ZKUtil.creatEphemeralNodeNoWatch(ja.getZkw(),
                            outputRetPathSeq, ToolUtil.toByte(line));
                    LOG.info("Result data: " + line);
                }
                // 捕获管道关闭引起的IO异常
            } catch (IOException e) {
                br.close();
                stdout.close();
            }

            // 写入命令执行完结信息EOF
            String outputRetPathSeq = outputRetPath + "_" + i;
            ZKUtil.creatEphemeralNodeNoWatch(ja.getZkw(), outputRetPathSeq,
                    ToolUtil.toByte("EOF"));

            LOG.info("Execute command " + id + " success: " + cmd);
        } catch (Exception e) {
            abort(desc, e);
        }
    }
}