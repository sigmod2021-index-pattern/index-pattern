package sigmod2021.esp.ql.impl.atomic;

import org.kohsuke.MetaInfServices;
import sigmod2021.esp.api.epa.pattern.symbol.Bindings;
import sigmod2021.esp.api.expression.arithmetic.atomic.IntConstant;
import sigmod2021.esp.expressions.arithmetic.constant.EVIntConstant;
import sigmod2021.esp.ql.ExpressionTranslator;
import sigmod2021.esp.ql.Translator;
import sigmod2021.esp.ql.TranslatorException;
import sigmod2021.event.EventSchema;

@MetaInfServices
public class IntConstantTranslator extends Translator<IntConstant, EVIntConstant> {

    public IntConstantTranslator() {
        super(IntConstant.class, EVIntConstant.class);
    }

    @Override
    protected EVIntConstant process(IntConstant in, ExpressionTranslator translator, EventSchema schema, Bindings bindings)
            throws TranslatorException {
        return new EVIntConstant(in.getValue());
    }

}
