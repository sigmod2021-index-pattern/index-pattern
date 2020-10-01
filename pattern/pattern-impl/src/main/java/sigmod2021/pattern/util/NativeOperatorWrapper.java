package sigmod2021.pattern.util;

import sigmod2021.common.EPException;
import sigmod2021.common.IncompatibleTypeException;
import sigmod2021.esp.api.bridge.EventChannel;
import sigmod2021.esp.api.epa.EPA;
import sigmod2021.esp.api.epa.UserDefinedEPA;
import sigmod2021.esp.bridges.nat.epa.NativeOperator;
import sigmod2021.event.Event;
import sigmod2021.event.EventSchema;
import sigmod2021.event.SchemaException;

public class NativeOperatorWrapper extends UserDefinedEPA {

    /**
     * The serialVersionUID
     */
    private static final long serialVersionUID = 1L;
    private final OperatorFactory factory;
    private NativeOperator op;

    /**
     * Creates a new NativeOperatorWrapper instance
     *
     * @param input
     * @param op
     */
    public NativeOperatorWrapper(OperatorFactory f) {
        this.factory = f;
    }

    /**
     * @{inheritDoc}
     */
    @Override
    public EventSchema computeOutputSchema(EventSchema... inputSchemas)
            throws SchemaException, IncompatibleTypeException {
        return this.factory.getDefinition().computeOutputSchema(inputSchemas);
    }

    /**
     * @{inheritDoc}
     */
    @Override
    public void initialize(EventChannel... inputStreams) throws EPException {

        this.op = this.factory.create(inputStreams);
        this.op.setCallback(NativeOperatorWrapper.this::publishResult);
    }

    /**
     * @{inheritDoc}
     */
    @Override
    public void process(EventChannel input, Event event) {
        op.process(input, event);
    }

    /**
     * @{inheritDoc}
     */
    @Override
    public void destroy(boolean flush) throws EPException {
        if (flush)
            op.flushState();
    }

    public static interface OperatorFactory {
        EPA getDefinition();

        NativeOperator create(EventChannel... inputSchemas);
    }

}
