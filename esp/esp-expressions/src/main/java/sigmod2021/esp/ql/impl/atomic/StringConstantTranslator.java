package sigmod2021.esp.ql.impl.atomic;

import org.kohsuke.MetaInfServices;
import sigmod2021.esp.api.epa.pattern.symbol.Bindings;
import sigmod2021.esp.api.expression.arithmetic.atomic.StringConstant;
import sigmod2021.esp.expressions.arithmetic.constant.EVStringConstant;
import sigmod2021.esp.ql.ExpressionTranslator;
import sigmod2021.esp.ql.Translator;
import sigmod2021.esp.ql.TranslatorException;
import sigmod2021.event.EventSchema;

@MetaInfServices
public class StringConstantTranslator extends Translator<StringConstant, EVStringConstant> {

    public StringConstantTranslator() {
        super(StringConstant.class, EVStringConstant.class);
    }

    @Override
    protected EVStringConstant process(StringConstant in, ExpressionTranslator translator, EventSchema schema, Bindings bindings)
            throws TranslatorException {
        return new EVStringConstant(in.getValue());
    }

}
