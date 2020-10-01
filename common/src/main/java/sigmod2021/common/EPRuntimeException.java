package sigmod2021.common;

/**
 *
 */
public class EPRuntimeException extends RuntimeException {

    /**
     *
     */
    private static final long serialVersionUID = 1L;

    /**
     * @param message
     * @param cause
     */
    public EPRuntimeException(String message, Throwable cause) {
        super(message, cause);
    }

    /**
     * @param message
     */
    public EPRuntimeException(String message) {
        super(message);
    }
}
