package sigmod2021.esp.ql.impl.bool;

import org.kohsuke.MetaInfServices;
import sigmod2021.esp.api.epa.pattern.symbol.Bindings;
import sigmod2021.esp.api.expression.logical.True;
import sigmod2021.esp.expressions.bool.EVTrue;
import sigmod2021.esp.ql.ExpressionTranslator;
import sigmod2021.esp.ql.Translator;
import sigmod2021.event.EventSchema;

@MetaInfServices
public class TrueTranslator extends Translator<True, EVTrue> {

    public TrueTranslator() {
        super(True.class, EVTrue.class);
    }

    @Override
    protected EVTrue process(True in, ExpressionTranslator translator, EventSchema schema, Bindings bindings) {
        return new EVTrue();
    }
}
