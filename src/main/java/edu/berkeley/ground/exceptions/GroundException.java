package edu.berkeley.ground.exceptions;

public class GroundException extends Exception {
    private String message;

    public GroundException(String message) {
        this.message = message;
    }

    public GroundException(Exception e) {
        this.message = e.getClass() + ": " + e.getMessage();
    }

    @Override
    public String getMessage() {
        return this.message;
    }
}
