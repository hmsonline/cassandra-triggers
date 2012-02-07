package com.hmsonline.cassandra.triggers;


public interface Trigger {

    public void process(LogEntry loEntry);
    
}
