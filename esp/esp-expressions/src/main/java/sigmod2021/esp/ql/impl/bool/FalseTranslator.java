package sigmod2021.esp.ql.impl.bool;

import org.kohsuke.MetaInfServices;
import sigmod2021.esp.api.epa.pattern.symbol.Bindings;
import sigmod2021.esp.api.expression.logical.False;
import sigmod2021.esp.expressions.bool.EVFalse;
import sigmod2021.esp.ql.ExpressionTranslator;
import sigmod2021.esp.ql.Translator;
import sigmod2021.event.EventSchema;

@MetaInfServices
public class FalseTranslator extends Translator<False, EVFalse> {

    public FalseTranslator() {
        super(False.class, EVFalse.class);
    }

    @Override
    protected EVFalse process(False in, ExpressionTranslator translator, EventSchema schema, Bindings bindings) {
        return new EVFalse();
    }
}
