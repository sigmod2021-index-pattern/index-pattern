package sigmod2021.esp.ql.impl.atomic;

import org.kohsuke.MetaInfServices;
import sigmod2021.esp.api.epa.pattern.symbol.Bindings;
import sigmod2021.esp.api.expression.arithmetic.atomic.LongConstant;
import sigmod2021.esp.expressions.arithmetic.constant.EVLongConstant;
import sigmod2021.esp.ql.ExpressionTranslator;
import sigmod2021.esp.ql.Translator;
import sigmod2021.esp.ql.TranslatorException;
import sigmod2021.event.EventSchema;

@MetaInfServices
public class LongConstantTranslator extends Translator<LongConstant, EVLongConstant> {

    public LongConstantTranslator() {
        super(LongConstant.class, EVLongConstant.class);
    }

    @Override
    protected EVLongConstant process(LongConstant in, ExpressionTranslator translator, EventSchema schema, Bindings bindings)
            throws TranslatorException {
        return new EVLongConstant(in.getValue());
    }

}
