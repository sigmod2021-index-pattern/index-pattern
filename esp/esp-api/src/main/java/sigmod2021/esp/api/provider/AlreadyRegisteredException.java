package sigmod2021.esp.api.provider;

import sigmod2021.common.EPException;

/**
 * Exception thrown if a stream or a query is registered more than once.
 */
public class AlreadyRegisteredException extends EPException {

    private static final long serialVersionUID = 1L;

    public AlreadyRegisteredException(String message, Throwable cause) {
        super(message, cause);
    }

    public AlreadyRegisteredException(String message) {
        super(message);
    }
}
