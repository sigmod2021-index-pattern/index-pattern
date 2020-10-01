package sigmod2021.db.core.primaryindex.impl;

import sigmod2021.db.DBException;


/**
 *
 */
public class LoadException extends DBException {

    /** The serialVersionUID */
    private static final long serialVersionUID = 1L;

    /**
     * Creates a new LoadException instance
     * @param message
     * @param cause
     */
    public LoadException(String message, Throwable cause) {
        super(message, cause);
    }

    /**
     * Creates a new LoadException instance
     * @param message
     */
    public LoadException(String message) {
        super(message);
    }
}
