package com.sohu.cloudno.comm;

import java.io.IOException;

/**
 * Thrown if the client can't connect to zookeeper
 */
public class ZooKeeperConnectionException extends IOException {
    private static final long serialVersionUID = 1L << 23 - 1L;

    /** default constructor */
    public ZooKeeperConnectionException() {
        super();
    }

    /**
     * Constructor
     * 
     * @param s
     *            message
     */
    public ZooKeeperConnectionException(String s) {
        super(s);
    }

    /**
     * Constructor taking another exception.
     * 
     * @param e
     *            Exception to grab data from.
     */
    public ZooKeeperConnectionException(String msg, Exception e) {
        super(msg, e);
    }
}