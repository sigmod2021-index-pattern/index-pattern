package sigmod2021.esp.bridges.nat.epa.impl.pattern.automaton;

import sigmod2021.common.IncompatibleTypeException;
import sigmod2021.esp.api.epa.pattern.symbol.Binding;
import sigmod2021.esp.api.epa.pattern.symbol.Binding.BindingTime;
import sigmod2021.esp.api.epa.pattern.symbol.Bindings;
import sigmod2021.esp.api.epa.pattern.symbol.Symbol;
import sigmod2021.esp.bindings.BoundVariables;
import sigmod2021.esp.bindings.EVBinding;
import sigmod2021.esp.bindings.EVBindings;
import sigmod2021.esp.expressions.bool.EVBooleanExpression;
import sigmod2021.esp.ql.ExpressionTranslator;
import sigmod2021.esp.ql.TranslatorException;
import sigmod2021.event.Event;
import sigmod2021.event.EventSchema;

import java.util.ArrayList;
import java.util.List;

/**
 *
 */
public class EVSymbol {

    private final char id;
    private final EVBooleanExpression condition;
    private final EVBindings bindings;
    private final int bindingsOffset;

    /**
     * Creates a new EVSymbol instance
     * @param id
     * @param condition
     * @param bindings
     */
    public EVSymbol(char id, EVBooleanExpression condition, EVBindings bindings, int bindingOffset) {
        this.id = id;
        this.condition = condition;
        this.bindings = bindings;
        this.bindingsOffset = bindingOffset;
    }

    public static EVSymbol create(Symbol symbol, EventSchema schema, Bindings previousBindings) throws TranslatorException, IncompatibleTypeException {
        EVBooleanExpression condition = ExpressionTranslator.INSTANCE.translateBooleanExpression(symbol.getCondition(), schema, previousBindings);

        List<EVBinding> bindings = new ArrayList<EVBinding>(symbol.getBindings().getNumberOfBindings());

        for (Binding b : symbol.getBindings()) {
            bindings.add(ExpressionTranslator.INSTANCE.translateVariableBinding(b, schema, previousBindings));
        }

        return new EVSymbol(symbol.getId(), condition, new EVBindings(bindings.toArray(new EVBinding[bindings.size()])), previousBindings.getNumberOfBindings());
    }

    /**
     * @return the id
     */
    public char getId() {
        return this.id;
    }


    /**
     * @return the condition
     */
    public EVBooleanExpression getCondition() {
        return this.condition;
    }


    /**
     * @return the bindings
     */
    public EVBindings getBindings() {
        return this.bindings;
    }

    public void updateBindings(Event event, BoundVariables variables) {
        for (int i = 0; i < bindings.getNumberOfBindings(); i++) {
            EVBinding evb = bindings.getBinding(i);
            if (evb.getBindingTime() == BindingTime.INCREMENTAL || variables.get(bindingsOffset + i) == null)
                variables.bind(bindingsOffset + i, evb.getExpression().eval(event, variables));
        }
    }

    //TODO: Dirty Hack
    public void updateBindings(List<Event> previousEvents, Event event, BoundVariables variables) {
        for (int i = 0; i < bindings.getNumberOfBindings(); i++) {
            EVBinding evb = bindings.getBinding(i);
            if (evb.getBindingTime() == BindingTime.INCREMENTAL || variables.get(bindingsOffset + i) == null)
                variables.bind(bindingsOffset + i, evb.getExpression().eval(previousEvents, event, variables));
        }
    }

}
