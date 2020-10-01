package sigmod2021.esp.ql.impl.arithmetic.numeric;

import org.kohsuke.MetaInfServices;
import sigmod2021.common.IncompatibleTypeException;
import sigmod2021.esp.api.epa.pattern.symbol.Bindings;
import sigmod2021.esp.api.expression.arithmetic.compund.numeric.Add;
import sigmod2021.esp.expressions.arithmetic.numeric.EVAdd;
import sigmod2021.esp.ql.ExpressionTranslator;
import sigmod2021.esp.ql.Translator;
import sigmod2021.esp.ql.TranslatorException;
import sigmod2021.event.EventSchema;
import sigmod2021.event.SchemaException;

@MetaInfServices
public class AddTranslator extends Translator<Add, EVAdd> {

    public AddTranslator() {
        super(Add.class, EVAdd.class);
    }

    @Override
    protected EVAdd process(Add in, ExpressionTranslator translator, EventSchema schema, Bindings bindings) throws TranslatorException, IncompatibleTypeException, SchemaException {
        return new EVAdd(
                translator.translateNumericExpression(in.getInput(0), schema, bindings),
                translator.translateNumericExpression(in.getInput(1), schema, bindings));
    }

}
