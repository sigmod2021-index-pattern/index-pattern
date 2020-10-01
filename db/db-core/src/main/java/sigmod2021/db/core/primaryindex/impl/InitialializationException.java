package sigmod2021.db.core.primaryindex.impl;

import sigmod2021.db.DBException;


/**
 *
 */
public class InitialializationException extends DBException {

    /** The serialVersionUID */
    private static final long serialVersionUID = 1L;

    /**
     * Creates a new InitialializationException instance
     * @param message
     * @param cause
     */
    public InitialializationException(String message, Throwable cause) {
        super(message, cause);
    }

    /**
     * Creates a new InitialializationException instance
     * @param message
     */
    public InitialializationException(String message) {
        super(message);
    }
}
