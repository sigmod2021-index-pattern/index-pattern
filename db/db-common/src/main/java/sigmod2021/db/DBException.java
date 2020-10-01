package sigmod2021.db;

public class DBException extends Exception {

    /** The serialVersionUID */
    private static final long serialVersionUID = 1L;

    /**
     * @param message
     */
    public DBException(String message) {
        super(message);
    }

    /**
     * @param message
     * @param cause
     */
    public DBException(String message, Throwable cause) {
        super(message, cause);
    }

}
