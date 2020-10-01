package sigmod2021.esp.ql.impl.arithmetic.numeric;

import org.kohsuke.MetaInfServices;
import sigmod2021.common.IncompatibleTypeException;
import sigmod2021.esp.api.epa.pattern.symbol.Bindings;
import sigmod2021.esp.api.expression.arithmetic.compund.numeric.Sqrt;
import sigmod2021.esp.expressions.arithmetic.numeric.EVSqrt;
import sigmod2021.esp.ql.ExpressionTranslator;
import sigmod2021.esp.ql.Translator;
import sigmod2021.esp.ql.TranslatorException;
import sigmod2021.event.EventSchema;
import sigmod2021.event.SchemaException;

@MetaInfServices
public class SqrtTranslator extends Translator<Sqrt, EVSqrt> {

    public SqrtTranslator() {
        super(Sqrt.class, EVSqrt.class);
    }

    @Override
    protected EVSqrt process(Sqrt in, ExpressionTranslator translator, EventSchema schema, Bindings bindings)
            throws TranslatorException, IncompatibleTypeException, SchemaException {
        return new EVSqrt(translator.translateNumericExpression(in.getInput(0), schema, bindings));
    }

}
