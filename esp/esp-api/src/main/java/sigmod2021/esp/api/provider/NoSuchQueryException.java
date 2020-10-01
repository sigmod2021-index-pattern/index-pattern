package sigmod2021.esp.api.provider;

import sigmod2021.common.EPException;

/**
 * Indicates that a query referenced by a user action
 * is not defined.
 */
public class NoSuchQueryException extends EPException {

    private static final long serialVersionUID = 1L;

    public NoSuchQueryException(String message) {
        super(message);
    }

    public NoSuchQueryException(String message, Throwable cause) {
        super(message, cause);
    }
}
