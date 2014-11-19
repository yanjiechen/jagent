package com.sohu.cloudno.comm;

public class Constant {
    // zk服务器root的路径
    public static final String ROOT = "/v0002";
    public static final String INSTANCE = "/instances";
    public static final String QUEUE = "/queue";
    public static final String JA_QUEUE = "/jaqueue";
    public static final String REMOTECONTROL = "/remotecontrol";
    public static final String INPUT = "/input";
    public static final String OUTPUT = "/output";
    public static final String FIELD_SPLIT = "&";

    // JAgent的配置文件路径
    public static final String JA_CONF = "/opt/sohu/ja/conf/ja.conf";

    // 监控数据库状态脚本位置
    public static final String SH_CHECK_DB = "/opt/sohu/ja/bin/cp_db_relation.sh";
    public static final String SH_SWITCHSTAT = "cat /etc/keepalived/var/slaveInitStatus";
    public static final String SH_HACONF = "/opt/sohu/ja/bin/haconf_row.sh";

    public static final String PHYSICAL_IP_FOR_WIN = "192.168.1.1";
}