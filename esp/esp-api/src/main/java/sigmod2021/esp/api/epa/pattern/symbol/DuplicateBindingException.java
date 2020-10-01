package sigmod2021.esp.api.epa.pattern.symbol;

import sigmod2021.event.SchemaException;

/**
 * Indicates a duplicate variable binding. This may be a variable
 * having the same name as an attribute of the event-stream or another
 * variable with the same name.
 */
public class DuplicateBindingException extends SchemaException {

    private static final long serialVersionUID = 1L;

    public DuplicateBindingException(String message, Throwable cause) {
        super(message, cause);
    }

    public DuplicateBindingException(String message) {
        super(message);
    }

}
