package sigmod2021.db.queries;

import sigmod2021.db.DBException;


/**
 *
 */
public class NoSuchEventException extends DBException {

    /** The serialVersionUID */
    private static final long serialVersionUID = 1L;

    /**
     * Creates a new NoSuchEventException instance
     * @param message
     */
    public NoSuchEventException(String message) {
        super(message);
        // TODO Auto-generated constructor stub
    }

    /**
     * Creates a new NoSuchEventException instance
     * @param message
     * @param cause
     */
    public NoSuchEventException(String message, Throwable cause) {
        super(message, cause);
        // TODO Auto-generated constructor stub
    }

}
