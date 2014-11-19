package com.sohu.cloudno.agent;

import com.sohu.cloudno.comm.Abortable;
import com.sohu.cloudno.comm.Conf;

/**
 * JA执行子任务的基类线程
 * 
 * @author yanjiechen
 * 
 */
public abstract class BaseThread extends Thread implements Abortable {
    protected JAgent ja;
    protected Conf conf;
    protected String desc;

    /**
     * 初始化
     * 
     * @param ja
     *            主线程标示，状态和是否退出通过这个参数同步到主线程
     * @param conf
     *            配置选项，比如重试次数
     * @param desc
     */
    BaseThread(JAgent ja, Conf conf, String desc) {
        this.ja = ja;
        this.conf = conf;
        this.desc = desc;
    }

    /**
     * 通知主main thread退出
     * 
     * @param msg
     *            Why we're aborting.
     * @param t
     *            Throwable that caused abort. Can be null.
     */
    public void abort(String why, Throwable e) {
        ja.abort(why, e);
    }

    /**
     * 设置名字，便于看调试的输出
     * 
     */
    public void setThreadName() {
        Thread.currentThread().setName(desc + Thread.currentThread().getId());
    }
}