package edu.berkeley.ground.exceptions;

public class EmptyResultException extends Exception {
  private String message;

  public EmptyResultException(String message) {
    this.message = message;
  }

  public EmptyResultException(Exception e) {
    this.message = e.getClass() + ": " + e.getMessage();
  }

  @Override
  public String getMessage() {
    return this.message;
  }
}
