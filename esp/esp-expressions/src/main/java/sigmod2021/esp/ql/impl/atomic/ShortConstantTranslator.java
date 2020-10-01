package sigmod2021.esp.ql.impl.atomic;

import org.kohsuke.MetaInfServices;
import sigmod2021.esp.api.epa.pattern.symbol.Bindings;
import sigmod2021.esp.api.expression.arithmetic.atomic.ShortConstant;
import sigmod2021.esp.expressions.arithmetic.constant.EVShortConstant;
import sigmod2021.esp.ql.ExpressionTranslator;
import sigmod2021.esp.ql.Translator;
import sigmod2021.esp.ql.TranslatorException;
import sigmod2021.event.EventSchema;

@MetaInfServices
public class ShortConstantTranslator extends Translator<ShortConstant, EVShortConstant> {

    public ShortConstantTranslator() {
        super(ShortConstant.class, EVShortConstant.class);
    }

    @Override
    protected EVShortConstant process(ShortConstant in, ExpressionTranslator translator, EventSchema schema, Bindings bindings)
            throws TranslatorException {
        return new EVShortConstant(in.getValue());
    }

}
