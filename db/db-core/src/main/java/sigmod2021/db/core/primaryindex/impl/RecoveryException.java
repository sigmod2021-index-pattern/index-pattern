package sigmod2021.db.core.primaryindex.impl;

import sigmod2021.db.DBException;


/**
 *
 */
public class RecoveryException extends DBException {

    /** The serialVersionUID */
    private static final long serialVersionUID = 1L;

    /**
     * Creates a new RecoveryException instance
     * @param message
     * @param cause
     */
    public RecoveryException(String message, Throwable cause) {
        super(message, cause);
    }

    /**
     * Creates a new RecoveryException instance
     * @param message
     */
    public RecoveryException(String message) {
        super(message);
    }
}
