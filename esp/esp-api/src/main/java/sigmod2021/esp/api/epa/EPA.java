package sigmod2021.esp.api.epa;

import sigmod2021.common.IncompatibleTypeException;
import sigmod2021.event.EventSchema;
import sigmod2021.event.SchemaException;

import java.io.Serializable;

/**
 * Abstract representation of event processing agents (EPA).
 */
public interface EPA extends Serializable {

    /**
     * A window has no input EPAs
     */
    public static final EPA[] EMPTY_EPA_ARRAY = {};

    /**
     * @return all input EPAs of this EPA
     */
    EPA[] getInputEPAs();

    /**
     * Changes the idx-th input of this EPA
     *
     * @param idx   the input index to change
     * @param input the new input EPA
     */
    void setInput(int idx, EPA input);

    default int getDepth() {
        int childDepth = 0;
        for (EPA c : getInputEPAs())
            childDepth = Math.max(childDepth, c.getDepth());
        return 1 + childDepth;
    }


    /**
     * @param inputSchemas the schemas of the input event streams
     * @return the output schema of this epa
     */
    EventSchema computeOutputSchema(EventSchema... inputSchemas) throws SchemaException, IncompatibleTypeException;

}
