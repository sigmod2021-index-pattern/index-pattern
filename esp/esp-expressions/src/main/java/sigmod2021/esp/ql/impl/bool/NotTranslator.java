package sigmod2021.esp.ql.impl.bool;

import org.kohsuke.MetaInfServices;
import sigmod2021.common.IncompatibleTypeException;
import sigmod2021.esp.api.epa.pattern.symbol.Bindings;
import sigmod2021.esp.api.expression.logical.Not;
import sigmod2021.esp.expressions.bool.EVBooleanExpression;
import sigmod2021.esp.expressions.bool.EVNot;
import sigmod2021.esp.ql.ExpressionTranslator;
import sigmod2021.esp.ql.Translator;
import sigmod2021.esp.ql.TranslatorException;
import sigmod2021.event.EventSchema;
import sigmod2021.event.SchemaException;

@MetaInfServices
public class NotTranslator extends Translator<Not, EVNot> {

    public NotTranslator() {
        super(Not.class, EVNot.class);
    }

    @Override
    protected EVNot process(Not in, ExpressionTranslator translator, EventSchema schema, Bindings bindings) throws TranslatorException, IncompatibleTypeException, SchemaException {
        EVBooleanExpression evin = translator.translateBooleanExpression(in.getInput(0), schema, bindings);
        return new EVNot(evin);
    }
}
