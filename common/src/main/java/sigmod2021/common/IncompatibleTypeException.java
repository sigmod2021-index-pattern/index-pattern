package sigmod2021.common;

/**
 * Indicates that types are incompatible (in terms of executing an expression).
 */
public class IncompatibleTypeException extends EPException {

    private static final long serialVersionUID = 1L;

    public IncompatibleTypeException(String message) {
        super(message);
    }

    public IncompatibleTypeException(String message, Throwable cause) {
        super(message, cause);
    }

}
