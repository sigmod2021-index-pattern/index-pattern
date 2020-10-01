package sigmod2021.esp.ql.impl.atomic;

import org.kohsuke.MetaInfServices;
import sigmod2021.esp.api.epa.pattern.symbol.Bindings;
import sigmod2021.esp.api.expression.arithmetic.atomic.DoubleConstant;
import sigmod2021.esp.expressions.arithmetic.constant.EVDoubleConstant;
import sigmod2021.esp.ql.ExpressionTranslator;
import sigmod2021.esp.ql.Translator;
import sigmod2021.esp.ql.TranslatorException;
import sigmod2021.event.EventSchema;

@MetaInfServices
public class DoubleConstantTranslator extends Translator<DoubleConstant, EVDoubleConstant> {

    public DoubleConstantTranslator() {
        super(DoubleConstant.class, EVDoubleConstant.class);
    }

    @Override
    protected EVDoubleConstant process(DoubleConstant in, ExpressionTranslator translator, EventSchema schema, Bindings bindings)
            throws TranslatorException {
        return new EVDoubleConstant(in.getValue());
    }

}
