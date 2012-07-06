package com.hmsonline.cassandra.triggers.dao;

import static org.junit.Assert.assertTrue;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.junit.Test;

/**
 * Unit test of {@link LogEntryStore}.
 *
 * @author Andrew Swan
 */
public class LogEntryStoreTest {
    
    private static final String INVALID_NAME_MESSAGE = "Host name '%s' does not match %s";
    
    // The pattern that a column family name must match in order to be valid
    private static final Pattern CF_NAME_PATTERN = Pattern.compile("[a-zA-Z0-9_]+");

    @Test
    public void hostNameShouldBeValidWithinColumnFamilyName() throws Exception {
        // Invoke
        String hostName = LogEntryStore.getHostName();
        
        // Check
        Matcher matcher = CF_NAME_PATTERN.matcher(hostName);
        String errorMessage =
                String.format(INVALID_NAME_MESSAGE, hostName, CF_NAME_PATTERN);
        assertTrue(errorMessage, matcher.matches());
    }
}
