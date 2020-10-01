package sigmod2021.esp.ql.impl.atomic;

import org.kohsuke.MetaInfServices;
import sigmod2021.esp.api.epa.pattern.symbol.Bindings;
import sigmod2021.esp.api.expression.arithmetic.atomic.FloatConstant;
import sigmod2021.esp.expressions.arithmetic.constant.EVFloatConstant;
import sigmod2021.esp.ql.ExpressionTranslator;
import sigmod2021.esp.ql.Translator;
import sigmod2021.esp.ql.TranslatorException;
import sigmod2021.event.EventSchema;

@MetaInfServices
public class FloatConstantTranslator extends Translator<FloatConstant, EVFloatConstant> {

    public FloatConstantTranslator() {
        super(FloatConstant.class, EVFloatConstant.class);
    }

    @Override
    protected EVFloatConstant process(FloatConstant in, ExpressionTranslator translator, EventSchema schema, Bindings bindings)
            throws TranslatorException {
        return new EVFloatConstant(in.getValue());
    }

}
