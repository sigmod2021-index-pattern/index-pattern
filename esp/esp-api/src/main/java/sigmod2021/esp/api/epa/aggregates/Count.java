package sigmod2021.esp.api.epa.aggregates;

import sigmod2021.esp.api.epa.window.Window;

/**
 * Calculates the number of events currently valid.
 * Validity is determined by the applied {@link Window}-EPA.
 */
public class Count extends Aggregate {

    private static final long serialVersionUID = 1L;

    /**
     * @param attributeIn  The attribute count on
     * @param attributeOut The name of the output attribute
     */
    public Count(String attributeIn, String attributeOut) {
        super(attributeIn, attributeOut);
    }

}
