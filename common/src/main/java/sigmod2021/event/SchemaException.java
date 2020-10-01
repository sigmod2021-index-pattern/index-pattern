package sigmod2021.event;

import sigmod2021.common.EPRuntimeException;

public class SchemaException extends EPRuntimeException {

    /**
     *
     */
    private static final long serialVersionUID = 1L;

    public SchemaException(String message) {
        super(message);
    }

    public SchemaException(String message, Throwable cause) {
        super(message, cause);
    }

}
