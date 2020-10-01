package sigmod2021.pattern.util;

import sigmod2021.esp.api.bridge.EventChannel;
import sigmod2021.esp.api.epa.Stream;
import sigmod2021.esp.api.epa.UserDefinedEPA;
import sigmod2021.event.Event;
import sigmod2021.event.EventSchema;

public class Input<T extends UserDefinedEPA> extends Stream implements EventChannel {

    /**
     * The serialVersionUID
     */
    private static final long serialVersionUID = 1L;

    private final EventSchema schema;

    private final T def;

    public Input(EventSchema schema, T def) {
        super("ReplayIn");
        this.schema = schema;
        this.def = def;
    }

    /**
     * @{inheritDoc}
     */
    @Override
    public EventSchema getSchema() {
        return schema;
    }

    /**
     * @{inheritDoc}
     */
    @Override
    public void push(Event event) {
        this.def.process(this, event);
    }
}
