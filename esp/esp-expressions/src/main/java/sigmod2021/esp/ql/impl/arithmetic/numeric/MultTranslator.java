package sigmod2021.esp.ql.impl.arithmetic.numeric;

import org.kohsuke.MetaInfServices;
import sigmod2021.common.IncompatibleTypeException;
import sigmod2021.esp.api.epa.pattern.symbol.Bindings;
import sigmod2021.esp.api.expression.arithmetic.compund.numeric.Multiply;
import sigmod2021.esp.expressions.arithmetic.numeric.EVMult;
import sigmod2021.esp.ql.ExpressionTranslator;
import sigmod2021.esp.ql.Translator;
import sigmod2021.esp.ql.TranslatorException;
import sigmod2021.event.EventSchema;
import sigmod2021.event.SchemaException;

@MetaInfServices
public class MultTranslator extends Translator<Multiply, EVMult> {

    public MultTranslator() {
        super(Multiply.class, EVMult.class);
    }

    @Override
    protected EVMult process(Multiply in, ExpressionTranslator translator, EventSchema schema, Bindings bindings) throws TranslatorException, IncompatibleTypeException, SchemaException {
        return new EVMult(
                translator.translateNumericExpression(in.getInput(0), schema, bindings),
                translator.translateNumericExpression(in.getInput(1), schema, bindings));
    }

}
