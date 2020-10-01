package sigmod2021.esp.api.provider;

import sigmod2021.common.EPException;

/**
 * Indicates that a stream referenced by a user action
 * is not defined.
 */
public class NoSuchStreamException extends EPException {

    private static final long serialVersionUID = 1L;

    public NoSuchStreamException(String message) {
        super(message);
    }

    public NoSuchStreamException(String message, Throwable cause) {
        super(message, cause);
    }
}
