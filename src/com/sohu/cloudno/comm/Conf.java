package com.sohu.cloudno.comm;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Properties;

import org.apache.log4j.Logger;

/**
 * 访问配置信息，一般包括zookeeper连接相关的参数 配置从文件加载和存储到本地
 * 
 * @author yanjiechen
 * 
 */
public class Conf {
    private static final Logger LOG = Logger.getLogger(Conf.class);

    protected Properties cfg;

    public static class ConfException extends Exception {

        private static final long serialVersionUID = -8644100917711502741L;

        public ConfException(String msg) {
            super(msg);
        }

        public ConfException(String msg, Exception e) {
            super(msg, e);
        }
    }

    public Conf() {
        cfg = new Properties();
    }

    /**
     * 从Constant.agentFile指定的文件名读取配置信息
     * 
     * @throws ConfException
     *             in cases of network failure
     */
    public void load() throws ConfException {
        load(Constant.JA_CONF);
    }

    /**
     * 从文件读取配置信息 如果文件不存着就新建立一个
     * 
     * @param fileName
     *            配置文件名
     * @throws ConfException
     *             in cases of network failure
     */
    public void load(String fileName) throws ConfException {
        LOG.info("load configuration file =" + fileName);
        try {
            File f = new File(fileName);
            if (!f.exists()) {
                f.createNewFile();
            }
            FileInputStream fis = new FileInputStream(fileName);
            try {
                cfg.load(fis);
            } finally {
                fis.close();
            }
        } catch (IOException e) {
            throw new ConfException("Error processing " + fileName, e);
        } catch (IllegalArgumentException e) {
            throw new ConfException("Error processing " + fileName, e);
        }
    }

    /**
     * 保存配置信息到文件，文件名为Constant.agentFile指定的文件名
     * 
     * @throws ConfException
     *             in cases of network failure
     */
    public void save() throws ConfException {
        save(Constant.JA_CONF);
    }

    /**
     * 保存配置信息到文件
     * 
     * @param fileName
     *            文件名
     * @throws ConfException
     *             in cases of network failure
     */
    public void save(String fileName) throws ConfException {
        try {
            FileOutputStream fos = null;
            try {
                fos = new FileOutputStream(fileName);
                cfg.store(fos, "please can not update manully");
            } finally {
                fos.close();
            }
        } catch (IOException e) {
            throw new ConfException("Error processing " + fileName, e);
        } catch (IllegalArgumentException e) {
            throw new ConfException("Error processing " + fileName, e);
        }
    }

    /**
     * 得到key对应的值
     * 
     * @param key
     * @return String 返回key对应的value值，没有value为null
     */
    public String get(String key) {
        String value = cfg.getProperty(key.trim());
        if (value != null)
            return value.trim();
        return null;
    }

    /**
     * 得到key对应的值
     * 
     * @param key
     *            <<<<<<< .mine
     * @return 返回key对应的value值，没有value为null
     */
    public String get(String key, String defaultValue) {
        String value = cfg.getProperty(key.trim());
        if (value != null)
            return value.trim();
        LOG.warn("coud not find key \"" + key
                + "\" in conf files, replace key with default value\""
                + defaultValue + "\"\n");
        return defaultValue;
    }

    /**
     * 得到key对应的值
     * 
     * @param key
     * @return 返回key对应的value值，没有value为缺省值
     * @return int 返回key对应的value值，没有value为缺省值
     */
    public int getInt(String key, int defaultValue) {
        String value = cfg.getProperty(key.trim());
        if (value != null) {
            try {
                return Integer.parseInt(value);
            } catch (Exception e) {
            }
        }
        LOG.warn("coud not find key \"" + key
                + "\" in conf files, replace key with default value\""
                + defaultValue + "\"\n");
        return defaultValue;
    }

    /**
     * 设置key对应的值
     * 
     * @param key
     * @param value
     */
    public void set(String key, String value) {
        cfg.setProperty(key.trim(), value.trim());
    }

    // 下面是测试代码
    public static void main(String[] args) throws Exception {
        Conf ag = new Conf();
        ag.load();
        ag.set("GroupName", "grp@99.26_3306");
        ag.set("InstanceName", "inst@99.80_3306");
        ag.set("ZKServer",
                "192.168.1.125:2181,192.168.1.128:2181,192.168.1.136:2181");
        ag.save();
        ag.load();
        LOG.info(ag.get("ZKServer"));
    }
}