package com.hmsonline.cassandra.triggers;

import java.util.concurrent.ConcurrentHashMap;

/**
 * @author <a href=irieksts@healthmarketscience.com>Isaac Rieksts</a>
 * 
 */
public class ProcessingManager {
    private static long TIME_TO_LIVE = 1000*60*10; // Ten minutes.
    private ConcurrentHashMap<String, Long> processing;

    public boolean isAlreadyBeingProcessed(String key) {
        Long histTime = getProcessing().get(key);
        long currTime = System.currentTimeMillis();
        if (histTime == null) {
            return false;
        } else {
            if ((currTime - histTime) > TIME_TO_LIVE) {
                remove(key);
                return false;
            } else {
                return true;
            }
        }
    }

    public void add(String key) {
        getProcessing().put(key, System.currentTimeMillis());
    }

    public void remove(String key) {
        getProcessing().remove(key);
    }

    /**
     * @return the history
     */
    protected ConcurrentHashMap<String, Long> getProcessing() {
        if (processing == null) {
            processing = new ConcurrentHashMap<String, Long>();
        }
        return processing;
    }

    /**
     * @param history the history to set
     */
    protected void setHistory(ConcurrentHashMap<String, Long> history) {
        this.processing = history;
    }

}
