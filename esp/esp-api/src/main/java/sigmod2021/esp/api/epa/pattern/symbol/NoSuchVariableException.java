package sigmod2021.esp.api.epa.pattern.symbol;

import sigmod2021.event.SchemaException;

/**
 * Indicates that a specific variable could not be found.
 */
public class NoSuchVariableException extends SchemaException {

    private static final long serialVersionUID = 1L;

    public NoSuchVariableException(String message, Throwable cause) {
        super(message, cause);
    }

    public NoSuchVariableException(String message) {
        super(message);
    }
}
