package edu.berkeley.ground.exceptions;

public class GroundDBException extends GroundException {
    public GroundDBException(String message){
        super(message);
    }

    public GroundDBException(Exception e) {
        super(e);
    }
}
