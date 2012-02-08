package com.hmsonline.cassandra.triggers;

public enum LogEntryStatus {
    PREPARING, COMMITTED, COMPLETE, ERROR; // ; is optional

    @Override
    public String toString() {
        return super.toString();
    }
}