package com.hmsonline.cassandra.triggers;

/**
 * A trigger that can be invoked upon a database mutation.
 */
public interface Trigger {

    /**
     * Causes this trigger to process the given {@link LogEntry}.
     *
     * @param logEntry the log entry to process (never <code>null</code>)
     */
    void process(LogEntry logEntry);
}
