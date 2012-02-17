package com.hmsonline.cassandra.triggers;

import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;

import org.apache.cassandra.utils.ByteBufferUtil;

public class ColumnOperation {
    private ByteBuffer name;
    private boolean isDelete;
        
    public ByteBuffer getName() {
        return name;
    }
    
    public void setName(ByteBuffer name) {
        this.name = name;
    }
    
    public boolean isDelete() {
        return isDelete;
    }
    
    public void setDelete(boolean isDelete) {
        this.isDelete = isDelete;
    }
    
    public void setOperationType(ByteBuffer value) throws CharacterCodingException{
        OperationType operation = OperationType.valueOf(ByteBufferUtil.string(value));
        this.setDelete(operation.equals(OperationType.DELETE));
    }
}
