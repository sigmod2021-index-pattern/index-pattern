package sigmod2021.esp.bridges.nat.epa.impl.pattern;

import sigmod2021.common.EPRuntimeException;
import sigmod2021.common.IncompatibleTypeException;
import sigmod2021.esp.api.bridge.EPACallback;
import sigmod2021.esp.api.bridge.EventChannel;
import sigmod2021.esp.api.epa.PatternMatcher;
import sigmod2021.esp.api.util.EventPartitioner;
import sigmod2021.esp.bridges.nat.epa.NativeOperator;
import sigmod2021.esp.bridges.nat.epa.impl.pattern.automaton.AutomatonPatternMatcher;
import sigmod2021.esp.bridges.nat.epa.impl.pattern.automaton.BufferAutomaton;
import sigmod2021.esp.ql.TranslatorException;
import sigmod2021.event.Event;
import sigmod2021.event.EventSchema;
import sigmod2021.event.SchemaException;

import java.util.HashMap;
import java.util.Map;

public class NativePatternMatcher implements NativeOperator {

    /**
     * The map of partition-keys to partitions
     */
    private final Map<Object, AutomatonPatternMatcher> partitions = new HashMap<>();

    private final BufferAutomaton automaton;

    private final EventSchema schemaOut;

    private final EventSchema schemaIn;

    private EPACallback operatorCallback;

    private EPACallback automatonCallback = new EPACallback() {

        @Override
        public void receive(Event event) {
            if (operatorCallback != null)
                operatorCallback.receive(event);
        }
    };

    private EventPartitioner partitioner;

    public NativePatternMatcher(PatternMatcher def, EventChannel input) throws SchemaException, IncompatibleTypeException, TranslatorException {
        this.schemaOut = def.computeOutputSchema(input.getSchema());
        this.schemaIn = input.getSchema();
        this.automaton = new BufferAutomaton(def, schemaIn);
        this.partitioner = new EventPartitioner(input.getSchema(), def.getPartitionBy());
    }

    private AutomatonPatternMatcher getPatternMatcher(Object key) {
        AutomatonPatternMatcher result = partitions.get(key);
        if (result == null) {
            result = new AutomatonPatternMatcher(automaton);
            result.setCallback(automatonCallback);
            partitions.put(key, result);
        }
        return result;
    }

    @Override
    public void process(EventChannel input, Event event) {
        getPatternMatcher(partitioner.buildKey(event)).match(event);
    }

    @Override
    public void setCallback(EPACallback callback) {
        this.operatorCallback = callback;
    }

    @Override
    public EventSchema getSchema() {
        return schemaOut;
    }

    /**
     * @{inheritDoc}
     */
    @Override
    public void flushState() {
    }

    public void clearState() {
        partitions.clear();
    }

}
