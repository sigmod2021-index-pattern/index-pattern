package sigmod2021.common;

/**
 *
 */
public class EPException extends Exception {

    /**
     *
     */
    private static final long serialVersionUID = 1L;

    /**
     * @param message
     * @param cause
     */
    public EPException(String message, Throwable cause) {
        super(message, cause);
    }

    /**
     * @param message
     */
    public EPException(String message) {
        super(message);
    }
}
