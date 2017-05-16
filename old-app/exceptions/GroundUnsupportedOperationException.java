package exceptions;

public class GroundUnsupportedOperationException extends GroundException {
  public GroundUnsupportedOperationException(Class<?> klass, String function) {
    super(klass.getName() + " does not support " + function);
  }
}
