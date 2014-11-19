package com.sohu.cloudno.comm;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PipedInputStream;
import java.io.PipedOutputStream;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.exec.CommandLine;
import org.apache.commons.exec.DefaultExecutor;
import org.apache.commons.exec.ExecuteWatchdog;
import org.apache.commons.exec.PumpStreamHandler;
import org.apache.log4j.Logger;

import com.sohu.cloudno.comm.Conf.ConfException;

public class ToolUtil {
    private static final Logger LOG = Logger.getLogger(ToolUtil.class);

    // 网络传输字符集
    private static final Charset CHARSET = Charset.forName("UTF-8");

    /**
     * 检查是否是合法的ip
     * 
     * @param ip
     * @return boolean
     */
    public static boolean checkIP(String ip) {
        try {
            String n = ip.substring(0, ip.indexOf('.'));
            if (Integer.parseInt(n) > 255)
                return false;
            ip = ip.substring(ip.indexOf('.') + 1);
            n = ip.substring(0, ip.indexOf('.'));
            if (Integer.parseInt(n) > 255)
                return false;
            ip = ip.substring(ip.indexOf('.') + 1);
            n = ip.substring(0, ip.indexOf('.'));
            if (Integer.parseInt(n) > 255)
                return false;
            n = ip.substring(ip.indexOf('.') + 1);
            if (Integer.parseInt(n) > 255)
                return false;
            return true;
        } catch (Exception e) {
            return false;
        }
    }

    /**
     * 把网络的字节流转换为字符串
     * 
     * @param b
     * @return String
     */
    public static String toStr(byte[] b) {
        return new String(b, CHARSET);
    }

    /**
     * 把字符串转换为网络的字节流
     * 
     * @param str
     * @return byte[]
     */
    public static byte[] toByte(String str) {
        return str.getBytes(CHARSET);
    }

    /**
     * 睡眠指定的毫秒数
     * 
     * @param millisecond
     * @return boolean
     */
    public static boolean sleep(long millisecond) {
        try {
            Thread.sleep(millisecond);
            return true;
        } catch (InterruptedException e) {
            LOG.warn("recive interrupted", e);
            return false;
        }
    }

    /**
     * 睡眠指定的毫秒数，如果被中断，抛出异常
     * 
     * @param millisecond
     * @throws InterruptedException
     */
    public static void sleepEx(long millisecond) throws InterruptedException {
        Thread.sleep(millisecond);
    }

    /**
     * 在指定时间运行OS的命令，得到OS命令的返回值 问题：win环境下超时好像会失效（和脚本大量输出有关系？）
     * 
     * @param cmd
     *            OS命令
     * @param timeout
     *            超时时间 0表示不设置超时
     * @param correctValues
     *            OS脚本定义为正常的返回值(比如int[] i={1,2,3};),null表示正常返回值为0
     * @return int -100表示脚本执行异常 ,-101表示超时
     */
    public static int runOSReturn(String cmd, int timeout, int[] correctValues) {
        ExecuteWatchdog watchdog = null;
        DefaultExecutor exec = new DefaultExecutor();
        int exitvalue = -100;
        CommandLine cl = CommandLine.parse(cmd);
        if (timeout > 0) {
            watchdog = new ExecuteWatchdog(timeout);
            exec.setWatchdog(watchdog);
        }
        if (correctValues != null)
            exec.setExitValues(correctValues);

        try {
            exitvalue = exec.execute(cl);
            // 如果是 correctValues内的，且是超时的
            if (watchdog != null)
                if (watchdog.killedProcess())
                    exitvalue = -101;
            return exitvalue;
        } catch (Exception e) {
            // 如果不是 correctValues内的
            if (watchdog != null)
                if (watchdog.killedProcess())
                    exitvalue = -101;
            LOG.warn("script " + cmd + " run abort!", e);
            return exitvalue;
        } finally {
            try {
                if (watchdog != null)
                    watchdog.destroyProcess();
            } catch (Exception e) {
            }
        }
    }

    /**
     * 在指定时间运行OS的命令，得到OS命令的输出结果, 注意如果脚本返回不是0(比如exit 1),会认为脚本异常而忽略os命令的返回值
     * 
     * @param cmd
     * @param timeout
     * @return String null表示脚本返回结果异常或者超时
     */
    public static String runOSCmd(String cmd, int timeout) {
        if (ToolUtil.isWindows())
            return ToolUtil.getFirstLine(cmd);

        String exitvalue = null;
        ExecuteWatchdog watchdog = null;
        ByteArrayOutputStream stdout = new ByteArrayOutputStream();
        PumpStreamHandler psh = new PumpStreamHandler(stdout);
        CommandLine cl = CommandLine.parse(cmd);
        DefaultExecutor exec = new DefaultExecutor();
        exec.setStreamHandler(psh);

        if (timeout > 0) {
            watchdog = new ExecuteWatchdog(timeout);
            exec.setWatchdog(watchdog);
        }

        try {
            exec.execute(cl);
            exitvalue = ToolUtil.replaceBlank(stdout.toString());
            // 如果是correctValues内的，且是超时的
            if (watchdog != null)
                if (watchdog.killedProcess())
                    exitvalue = "T";
            return exitvalue;
        } catch (Exception e) {
            // 如果不是correctValues内的
            if (watchdog != null)
                if (watchdog.killedProcess())
                    exitvalue = "T";
            LOG.warn("script " + cmd + " run abort!", e);
            return exitvalue;
        } finally {
            try {
                stdout.close();
                if (watchdog != null)
                    watchdog.destroyProcess();
            } catch (Exception e) {
            }
        }
    }

    /**
     * 在指定时间运行OS的命令，得到OS命令的输出结果, 注意如果脚本返回不是0(比如exit 1),会认为脚本异常而忽略os命令的返回值
     * 
     * @param cmd
     * @param timeout
     * @return String null表示脚本返回结果异常或者超时
     */
    public static String runOSCmdRaw(String cmd, long timeout) {
        if (ToolUtil.isWindows())
            return ToolUtil.getFirstLine(cmd);

        String exitvalue = null;
        timeout = timeout <= 0 ? ExecuteWatchdog.INFINITE_TIMEOUT : timeout;
        ExecuteWatchdog watchdog = new ExecuteWatchdog(timeout);
        ByteArrayOutputStream stdout = new ByteArrayOutputStream();
        PumpStreamHandler psh = new PumpStreamHandler(stdout);
        CommandLine cl = CommandLine.parse(cmd);
        DefaultExecutor exec = new DefaultExecutor();
        exec.setStreamHandler(psh);
        exec.setWatchdog(watchdog);

        try {
            exec.execute(cl);
            exitvalue = stdout.toString();
            // 如果是correctValues内的，且是超时的
            if (watchdog.killedProcess())
                exitvalue = null;
            return exitvalue;
        } catch (Exception e) {
            // 如果不是correctValues内的
            if (watchdog.killedProcess())
                exitvalue = null;
            LOG.warn("script " + cmd + " run abort!", e);
            return exitvalue;
        } finally {
            try {
                stdout.close();
                watchdog.destroyProcess();
            } catch (Exception e) {
            }
        }
    }

    /**
     * 在指定时间运行OS的命令，得到OS命令的输出结果, 注意如果脚本返回不是0(比如exit 1),会认为脚本异常而忽略os命令的返回值
     * 这个方法不能在win平台使用，所以读文件替代
     * 
     * @param cmd
     * @param timeout
     *            单位milliseconds
     * @return List 表示脚本返回结果
     */
    public static List<String> runOSCmdReturnListOld(String cmd, long timeout) {
        if (ToolUtil.isWindows())
            return ToolUtil.getAllLine(cmd);

        List<String> ret = null;
        ExecuteWatchdog watchdog = null;
        PipedOutputStream stdout = new PipedOutputStream();
        PumpStreamHandler psh = new PumpStreamHandler(stdout);
        LOG.debug(cmd);
        CommandLine cl = CommandLine.parse(cmd);
        DefaultExecutor exec = new DefaultExecutor();
        exec.setStreamHandler(psh);

        if (timeout > 0) {
            watchdog = new ExecuteWatchdog(timeout);
            exec.setWatchdog(watchdog);
        }

        try {
            exec.execute(cl);

            BufferedReader br = new BufferedReader(new InputStreamReader(
                    new PipedInputStream(stdout)));

            // 按照执行结果逐行写入ArrayList
            ret = new ArrayList<String>();
            LOG.debug("1");
            try {
                String line = null;
                LOG.debug("2");
                while ((line = br.readLine()) != null) {
                    LOG.debug("3");
                    ret.add(line);
                }
                LOG.debug("3");
                // 捕获管道关闭引起的IO异常
            } catch (IOException e) {
                br.close();
                // stdout.close();
            }
            // 如果是correctValues内的，且是超时的
            if (watchdog != null)
                if (watchdog.killedProcess())
                    ret = null;
            return ret;
        } catch (Exception e) {
            // 如果不是correctValues内的
            if (watchdog != null)
                if (watchdog.killedProcess())
                    ret = null;
            LOG.warn("script " + cmd + " run abort!", e);
            return ret;
        } finally {
            try {
                try {
                    stdout.close();
                } catch (IOException e) {
                }
                ;
                if (watchdog != null)
                    watchdog.destroyProcess();
            } catch (Exception e) {
            }
        }
    }

    /**
     * 在指定时间运行OS的命令，得到OS命令的输出结果, 注意如果脚本返回不是0(比如exit 1),会认为脚本异常而忽略os命令的返回值
     * 这个方法不能在win平台使用，所以读文件替代
     * 
     * @param cmd
     * @param timeout
     *            单位milliseconds
     * @return List 表示脚本返回结果,null表示超时
     */
    public static List<String> runOSCmdReturnList(String cmd, long timeout) {
        if (ToolUtil.isWindows())
            return ToolUtil.getAllLine(cmd);

        List<String> ret = null;
        ExecuteWatchdog watchdog = null;
        ByteArrayOutputStream stdout = new ByteArrayOutputStream();
        PumpStreamHandler psh = new PumpStreamHandler(stdout);
        CommandLine cl = CommandLine.parse(cmd);
        DefaultExecutor exec = new DefaultExecutor();
        exec.setStreamHandler(psh);

        if (timeout > 0) {
            watchdog = new ExecuteWatchdog(timeout);
            exec.setWatchdog(watchdog);
        }

        try {
            exec.execute(cl);

            BufferedReader br = new BufferedReader(new InputStreamReader(
                    new ByteArrayInputStream(stdout.toByteArray())));

            // 按照执行结果逐行写入ArrayList
            ret = new ArrayList<String>();
            try {
                String line = null;
                while ((line = br.readLine()) != null) {
                    ret.add(line);
                }
                // 捕获管道关闭引起的IO异常
            } catch (IOException e) {
                br.close();
                // stdout.close();
            }
            // 如果是correctValues内的，且是超时的
            if (watchdog != null)
                if (watchdog.killedProcess())
                    ret = null;
            return ret;
        } catch (Exception e) {
            // 如果不是correctValues内的,或者超时异常
            if (watchdog != null)
                if (watchdog.killedProcess())
                    ret = null;
            LOG.warn("script " + cmd + " run abort," + ToolUtil.exceptionMsg(e), e);
            return ret;
        } finally {
            try {
                stdout.close();
            } catch (IOException e) {
                
            }
            if (watchdog != null)
                watchdog.destroyProcess();
        }
    }

    /**
     * 从配置文件中读第一行，不包括注释行
     * 
     * @param fileName
     * @return String null表示没有读任何信息到或者文件不存着
     */
    public static String getFirstLine(String fileName) {
        String line = null;
        BufferedReader br = null;

        try {
            // Construct the BufferedReader object
            br = new BufferedReader(new FileReader(fileName));

            while ((line = br.readLine()) != null) {
                // 跳过注释行
                if (!line.startsWith("#"))
                    break;
                /*
                 * StringTokenizer st = new StringTokenizer(line); while
                 * (st.hasMoreTokens()) { String ele=st.nextToken();
                 * System.out.println(ele+" "+ele.length());
                 */
            }
        } catch (FileNotFoundException e) {
            LOG.warn("file " + fileName + " not found", e);
        } catch (IOException e) {
            LOG.warn("file " + fileName + " io error", e);
        } finally {
            // Close the BufferedReader
            try {
                if (br != null)
                    br.close();
            } catch (IOException e) {
                LOG.warn("file " + fileName + " io error", e);
            }
        }
        return line;
    }

    /**
     * 从配置文件中读第一行，不包括注释行
     * 
     * @param fileName
     * @return String null表示没有读任何信息到或者文件不存着
     */
    public static List<String> getAllLine(String fileName) {
        String line = null;
        List<String> ret = new ArrayList<String>();
        BufferedReader br = null;

        try {
            // Construct the BufferedReader object
            br = new BufferedReader(new FileReader(fileName));

            while ((line = br.readLine()) != null) {
                ret.add(line);
                /*
                 * StringTokenizer st = new StringTokenizer(line); while
                 * (st.hasMoreTokens()) { String ele=st.nextToken();
                 * System.out.println(ele+" "+ele.length());
                 */
            }
        } catch (FileNotFoundException e) {
            LOG.warn("file " + fileName + " not found", e);
        } catch (IOException e) {
            LOG.warn("file " + fileName + " io error", e);
        } finally {
            // Close the BufferedReader
            try {
                if (br != null)
                    br.close();
            } catch (IOException e) {
                LOG.warn("file " + fileName + " io error", e);
            }
        }
        return ret;
    }

    /**
     * 判断当前操作系统是不是window
     * 
     * @return boolean www.2cto.com
     */
    public static boolean isWindows() {
        boolean flag = false;
        if (System.getProperties().getProperty("os.name").toUpperCase()
                .indexOf("WINDOWS") != -1) {
            flag = true;
        }
        return flag;
    }

    /**
     * 分析 key value对，得到value值
     * 
     * @param line
     *            key value对
     * @return String null表示key value对格式有问题或者key value对本身就是空
     */
    public static String getValue(String line) {
        String ret = null;
        try {
            ret = ToolUtil.replaceBlank(line.substring(line.indexOf('=') + 1));
        } catch (Exception e) {
            LOG.warn(
                    "\""
                            + line
                            + "\" format is error, expect the following format \"result=value\" ",
                    e);
            ret = null;
        }
        return ret;
    }

    /**
     * 分析 key value对，得到value值
     * 
     * @param line
     *            key value对
     * @return String null表示key value对格式有问题或者key value对本身就是空
     */
    public static String getValueRev(String line) {
        // checkStr = checkStr.substring(checkStr.indexOf('.')+ 1);
        String ret = null;
        try {
            ret = ToolUtil
                    .replaceBlank(line.substring(line.lastIndexOf('=') + 1));
        } catch (Exception e) {
            LOG.warn(
                    "\""
                            + line
                            + "\" format is error, expect the following format \"result=value\" ",
                    e);
            ret = null;
        }
        return ret;
    }

    /**
     * 检查返回值是否符合要求
     * 
     * @param val
     *            要检查的值，比如"ALIVE"
     * @param correctValues
     *            正确值的列表,比如 String wvipCorrect[] = { "ALIVE", "DEAD" }
     * @param desc
     *            描述，作为log输出用
     * @return int 0：符合要求 1: 不符合要求 2：val为null 3: correctValues为null
     */
    public static int checkReturn(String val, String[] correctValues,
            String desc) {
        if (val == null)
            return 2;
        if (correctValues == null)
            return 3;
        int i = 0;
        for (i = 0; i < correctValues.length; i++) {
            if (correctValues[i].equals(val))
                break;
        }

        if (i == correctValues.length) {
            String expect = "[";
            for (String correctValue : correctValues) {
                expect = expect + correctValue + ",";
            }
            expect = expect + "]";
            LOG.warn(desc + " content is error, error values is \"" + val
                    + "\", expected values are \"" + expect + "\"");
            return 1;
        } else {
            return 0;
        }
    }

    /**
     * 去除字符串中的空格、回车、换行符、制表符
     * 
     * @param str
     * @return String
     */
    public static String replaceBlank(String str) {
        String dest = "";
        if (str != null) {
            Pattern p = Pattern.compile("\\s*|\t|\r|\n");
            Matcher m = p.matcher(str);
            dest = m.replaceAll("");
        }
        return dest;
    }

    /**
     * 得到本机配置的物理ip
     * 
     * @return String
     * @throws UnknownHostException
     */
    public static String getPhysicalIp() throws UnknownHostException {
        return InetAddress.getLocalHost().getHostAddress();
    }

    /**
     * 调用指定的脚本得到本机配置的物理ip
     * 
     * @param script
     *            脚本的位置
     * @return String
     * @throws ConfException
     */
    public static String getPhysicalIp(String script) throws ConfException {
        String ip = null;
        if (ToolUtil.isWindows()) {
            return Constant.PHYSICAL_IP_FOR_WIN;
        }
        ip = getValue(runOSCmd(script, 0));
        if (!checkIP(ip))
            throw new ConfException("Format of physical ip " + ip
                    + "is wrong. ip length is " + ip.length()
                    + " maybe include blank space?");
        return ip;
    }

    /**
     * 获取详细的异常信息
     * 
     * @param e
     * @return StringWriter
     */
    public static StringWriter exceptionMsg(Throwable e) {
        StringWriter sw = new StringWriter();
        PrintWriter pw = new PrintWriter(sw);
        e.printStackTrace(pw);
        return sw;
    }
}