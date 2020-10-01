package sigmod2021.esp.ql.impl.arithmetic.numeric;

import org.kohsuke.MetaInfServices;
import sigmod2021.common.IncompatibleTypeException;
import sigmod2021.esp.api.epa.pattern.symbol.Bindings;
import sigmod2021.esp.api.expression.arithmetic.compund.numeric.Divide;
import sigmod2021.esp.expressions.arithmetic.numeric.EVDivide;
import sigmod2021.esp.ql.ExpressionTranslator;
import sigmod2021.esp.ql.Translator;
import sigmod2021.esp.ql.TranslatorException;
import sigmod2021.event.EventSchema;
import sigmod2021.event.SchemaException;

@MetaInfServices
public class DivideTranslator extends Translator<Divide, EVDivide> {

    public DivideTranslator() {
        super(Divide.class, EVDivide.class);
    }

    @Override
    protected EVDivide process(Divide in, ExpressionTranslator translator, EventSchema schema, Bindings bindings) throws TranslatorException, IncompatibleTypeException, SchemaException {
        return new EVDivide(
                translator.translateNumericExpression(in.getInput(0), schema, bindings),
                translator.translateNumericExpression(in.getInput(1), schema, bindings));
    }

}
