package sigmod2021.esp.api.epa.aggregates;

import sigmod2021.esp.api.epa.window.Window;

/**
 * Calculates the sum on the given attribute for
 * all valid events. Validity is determined by the applied
 * {@link Window}-EPA.
 * <br>
 * This aggregate may be applied to numeric attribtues only.
 */
public class Sum extends Aggregate {

    private static final long serialVersionUID = 1L;

    /**
     * @param attributeIn  The attribute to calculate the sum on
     * @param attributeOut The name of the output attribute
     */
    public Sum(String attributeIn, String attributeOut) {
        super(attributeIn, attributeOut);
    }


}
