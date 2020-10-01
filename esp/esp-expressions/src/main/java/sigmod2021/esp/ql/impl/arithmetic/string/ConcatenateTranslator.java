package sigmod2021.esp.ql.impl.arithmetic.string;

import org.kohsuke.MetaInfServices;
import sigmod2021.common.IncompatibleTypeException;
import sigmod2021.esp.api.epa.pattern.symbol.Bindings;
import sigmod2021.esp.api.expression.ArithmeticExpression;
import sigmod2021.esp.api.expression.arithmetic.compund.string.StringConcatenate;
import sigmod2021.esp.expressions.arithmetic.string.EVConcatenate;
import sigmod2021.esp.ql.ExpressionTranslator;
import sigmod2021.esp.ql.Translator;
import sigmod2021.esp.ql.TranslatorException;
import sigmod2021.event.Attribute.DataType;
import sigmod2021.event.EventSchema;
import sigmod2021.event.SchemaException;

/**
 *
 */
@MetaInfServices
public class ConcatenateTranslator extends Translator<StringConcatenate, EVConcatenate> {

    public ConcatenateTranslator() {
        super(StringConcatenate.class, EVConcatenate.class);
    }

    @Override
    protected EVConcatenate process(StringConcatenate in, ExpressionTranslator translator, EventSchema schema, Bindings bindings)
            throws TranslatorException, SchemaException, IncompatibleTypeException {
        ArithmeticExpression left = in.getInput(0);
        ArithmeticExpression right = in.getInput(1);
        if (left.getDataType(schema, bindings) == DataType.STRING && right.getDataType(schema, bindings) == DataType.STRING)
            return new EVConcatenate(translator.translateStringExpression(left, schema, bindings), translator.translateStringExpression(right, schema, bindings));
        else
            throw new TranslatorException("Both inputs must be string types ");


    }

}
