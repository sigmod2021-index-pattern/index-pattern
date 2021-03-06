package sigmod2021.esp.ql.impl.predicate;

import org.kohsuke.MetaInfServices;
import sigmod2021.common.IncompatibleTypeException;
import sigmod2021.esp.api.epa.pattern.symbol.Bindings;
import sigmod2021.esp.api.expression.ArithmeticExpression;
import sigmod2021.esp.api.expression.predicate.Greater;
import sigmod2021.esp.expressions.arithmetic.EVNumericExpression;
import sigmod2021.esp.expressions.arithmetic.EVStringExpression;
import sigmod2021.esp.expressions.predicate.EVPredicate;
import sigmod2021.esp.expressions.predicate.numeric.EVNumericGreater;
import sigmod2021.esp.expressions.predicate.string.EVStringGreater;
import sigmod2021.esp.ql.ExpressionTranslator;
import sigmod2021.esp.ql.Translator;
import sigmod2021.esp.ql.TranslatorException;
import sigmod2021.event.Attribute.DataType;
import sigmod2021.event.EventSchema;
import sigmod2021.event.SchemaException;

@MetaInfServices
public class GreaterTranslator extends Translator<Greater, EVPredicate> {

    public GreaterTranslator() {
        super(Greater.class, EVPredicate.class);
    }

    @Override
    protected EVPredicate process(Greater in, ExpressionTranslator translator, EventSchema schema, Bindings bindings)
            throws TranslatorException, IncompatibleTypeException, SchemaException {
        ArithmeticExpression l = in.getInput(0);
        ArithmeticExpression r = in.getInput(1);
        DataType lt = l.getDataType(schema, bindings);
        DataType rt = r.getDataType(schema, bindings);

        DataType gct = DataType.getGCT(lt, rt);

        if (gct.isNumeric()) {
            EVNumericExpression tl = translator.translateNumericExpression(l, schema, bindings);
            EVNumericExpression tr = translator.translateNumericExpression(r, schema, bindings);
            return new EVNumericGreater(gct, tl, tr);
        } else if (gct == DataType.STRING) {
            EVStringExpression tl = translator.translateStringExpression(l, schema, bindings);
            EVStringExpression tr = translator.translateStringExpression(r, schema, bindings);
            return new EVStringGreater(tl, tr);
        } else
            throw new IncompatibleTypeException("No implementation for Greater with input-types: " + lt + ", " + rt);
    }
}
