package sigmod2021.esp.ql.impl.arithmetic.numeric;

import org.kohsuke.MetaInfServices;
import sigmod2021.common.IncompatibleTypeException;
import sigmod2021.esp.api.epa.pattern.symbol.Bindings;
import sigmod2021.esp.api.expression.arithmetic.compund.numeric.ATan2;
import sigmod2021.esp.expressions.arithmetic.numeric.EVATan2;
import sigmod2021.esp.ql.ExpressionTranslator;
import sigmod2021.esp.ql.Translator;
import sigmod2021.esp.ql.TranslatorException;
import sigmod2021.event.EventSchema;
import sigmod2021.event.SchemaException;

@MetaInfServices
public class ATan2Translator extends Translator<ATan2, EVATan2> {

    public ATan2Translator() {
        super(ATan2.class, EVATan2.class);
    }

    @Override
    protected EVATan2 process(ATan2 in, ExpressionTranslator translator, EventSchema schema, Bindings bindings)
            throws TranslatorException, IncompatibleTypeException, SchemaException {
        return new EVATan2(translator.translateNumericExpression(in.getInput(0), schema, bindings),
                translator.translateNumericExpression(in.getInput(1), schema, bindings));
    }

}
