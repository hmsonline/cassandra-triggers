package com.hmsonline.cassandra.triggers.dao;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class ErrorLog extends LogEntryStore {
    private static Logger logger = LoggerFactory.getLogger(ErrorLog.class);

    public static final String KEYSPACE = "triggers";
    public static final String COLUMN_FAMILY_PREFIX = "ErrorLog";
    public static final int MAX_NUMBER_COLUMNS = 100;
    public static final int BATCH_SIZE = 1000;

    private static ErrorLog instance = null;

    public ErrorLog() throws Exception {
        super(KEYSPACE, COLUMN_FAMILY_PREFIX);
        logger.warn("Instantiated error log.");
    }

    public static synchronized ErrorLog getErrorLog() throws Exception {
        if (instance == null) {
            instance = new ErrorLog();
        }
        return instance;
    }
}
