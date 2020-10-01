package sigmod2021.db;

public class DBRuntimeException extends RuntimeException {

    /** The serialVersionUID */
    private static final long serialVersionUID = 1L;

    /**
     * @param message
     * @param cause
     */
    public DBRuntimeException(String message, Throwable cause) {
        super(message, cause);
    }

    /**
     * @param message
     */
    public DBRuntimeException(String message) {
        super(message);
    }

}
