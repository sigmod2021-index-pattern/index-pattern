package sigmod2021.esp.ql;

import sigmod2021.common.EPException;

public class TranslatorException extends EPException {


    /**
     *
     */
    private static final long serialVersionUID = 1L;

    public TranslatorException(String message) {
        super(message);
    }

    public TranslatorException(String message, Throwable cause) {
        super(message, cause);
    }

}
