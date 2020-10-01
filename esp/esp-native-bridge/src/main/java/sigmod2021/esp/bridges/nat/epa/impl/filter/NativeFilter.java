package sigmod2021.esp.bridges.nat.epa.impl.filter;

import sigmod2021.common.IncompatibleTypeException;
import sigmod2021.esp.api.bridge.EPACallback;
import sigmod2021.esp.api.bridge.EventChannel;
import sigmod2021.esp.api.epa.Filter;
import sigmod2021.esp.api.epa.pattern.symbol.Bindings;
import sigmod2021.esp.bridges.nat.epa.NativeOperator;
import sigmod2021.esp.expressions.bool.EVBooleanExpression;
import sigmod2021.esp.ql.ExpressionTranslator;
import sigmod2021.esp.ql.TranslatorException;
import sigmod2021.event.Event;
import sigmod2021.event.EventSchema;

public class NativeFilter implements NativeOperator {

    private final EventSchema schema;

    private final EVBooleanExpression condition;

    private EPACallback callback;

    public NativeFilter(Filter def, EventChannel input) throws TranslatorException, IncompatibleTypeException {
        this.schema = def.computeOutputSchema(input.getSchema());
        this.condition = new ExpressionTranslator().translateBooleanExpression(def.getCondition(), input.getSchema(), new Bindings());

    }

    @Override
    public void process(EventChannel input, Event event) {
        if (condition.eval(event))
            callback.receive(event);
    }

    @Override
    public void setCallback(EPACallback callback) {
        this.callback = callback;
    }

    @Override
    public EventSchema getSchema() {
        return schema;
    }

    /**
     * @{inheritDoc}
     */
    @Override
    public void flushState() {
    }

}
