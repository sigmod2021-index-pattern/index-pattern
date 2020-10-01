package sigmod2021.esp.ql.impl.bool;

import org.kohsuke.MetaInfServices;
import sigmod2021.common.IncompatibleTypeException;
import sigmod2021.esp.api.epa.pattern.symbol.Bindings;
import sigmod2021.esp.api.expression.logical.And;
import sigmod2021.esp.expressions.bool.EVAnd;
import sigmod2021.esp.expressions.bool.EVBooleanExpression;
import sigmod2021.esp.ql.ExpressionTranslator;
import sigmod2021.esp.ql.Translator;
import sigmod2021.esp.ql.TranslatorException;
import sigmod2021.event.EventSchema;
import sigmod2021.event.SchemaException;

@MetaInfServices
public class AndTranslator extends Translator<And, EVAnd> {

    public AndTranslator() {
        super(And.class, EVAnd.class);
    }

    @Override
    protected EVAnd process(And in, ExpressionTranslator translator, EventSchema schema, Bindings bindings) throws TranslatorException, IncompatibleTypeException, SchemaException {
        EVBooleanExpression left = translator.translateBooleanExpression(in.getInput(0), schema, bindings);
        EVBooleanExpression right = translator.translateBooleanExpression(in.getInput(1), schema, bindings);
        return new EVAnd(left, right);
    }
}
