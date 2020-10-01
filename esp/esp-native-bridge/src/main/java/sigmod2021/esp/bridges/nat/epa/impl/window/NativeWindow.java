package sigmod2021.esp.bridges.nat.epa.impl.window;

import sigmod2021.esp.api.bridge.EPACallback;
import sigmod2021.esp.api.bridge.EventChannel;
import sigmod2021.esp.api.epa.pattern.symbol.NoSuchVariableException;
import sigmod2021.esp.api.epa.window.CountWindow;
import sigmod2021.esp.api.epa.window.PartitionedCountWindow;
import sigmod2021.esp.api.epa.window.TimeWindow;
import sigmod2021.esp.api.epa.window.Window;
import sigmod2021.esp.bridges.nat.epa.NativeOperator;
import sigmod2021.event.EventSchema;
import sigmod2021.event.SchemaException;

public abstract class NativeWindow implements NativeOperator {

    /**
     * The size of the window (usage depends on concrete window-type)
     */
    protected final long size;
    /**
     * Tells the jump-size on updates
     */
    protected final long jump;
    /**
     * The schema of this window's stream
     */
    private final EventSchema schema;
    /**
     * The receiver for results of this window
     */
    protected EPACallback callback;

    /**
     * Constructs a new NativeWindow-instance
     *
     * @param name     the internal name of this window
     * @param def      the stream definition
     * @param schema   the event-schema
     * @param receiver the receiver for results of this window
     */
    protected NativeWindow(EventChannel input, Window def) {
        this.schema = input.getSchema();
        this.size = def.getSize();
        this.jump = def.getJump();
    }

    /**
     * @param schema the schema of the raw stream
     * @param def    the window definition
     * @return the new window operator
     * @throws NoSuchVariableException
     */
    public static NativeWindow buildWindow(EventChannel input, Window def) throws SchemaException {
        // Create time-window
        if (def instanceof TimeWindow) {
            return new NativeTimeWindow(input, (TimeWindow) def);
        }
        // Create Count-window
        else if (def instanceof CountWindow) {
            return new NativeCountWindow(input, (CountWindow) def);
        }
        // Create partitioned-count-window
        else if (def instanceof PartitionedCountWindow) {
            return new NativePartitionedCountWindow(input, (PartitionedCountWindow) def);
        } else {
            throw new IllegalArgumentException("Unknown Window-Type: " + def.getClass().getName());
        }
    }

    @Override
    public EventSchema getSchema() {
        return schema;
    }

    @Override
    public void setCallback(EPACallback callback) {
        this.callback = callback;
    }

}
