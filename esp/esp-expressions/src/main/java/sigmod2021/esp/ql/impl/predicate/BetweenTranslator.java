package sigmod2021.esp.ql.impl.predicate;

import org.kohsuke.MetaInfServices;
import sigmod2021.common.IncompatibleTypeException;
import sigmod2021.esp.api.epa.pattern.symbol.Bindings;
import sigmod2021.esp.api.expression.ArithmeticExpression;
import sigmod2021.esp.api.expression.predicate.Between;
import sigmod2021.esp.expressions.arithmetic.EVNumericExpression;
import sigmod2021.esp.expressions.arithmetic.EVStringExpression;
import sigmod2021.esp.expressions.predicate.EVPredicate;
import sigmod2021.esp.expressions.predicate.numeric.EVNumericBetween;
import sigmod2021.esp.expressions.predicate.string.EVStringBetween;
import sigmod2021.esp.ql.ExpressionTranslator;
import sigmod2021.esp.ql.Translator;
import sigmod2021.esp.ql.TranslatorException;
import sigmod2021.event.Attribute.DataType;
import sigmod2021.event.EventSchema;
import sigmod2021.event.SchemaException;

@MetaInfServices
public class BetweenTranslator extends Translator<Between, EVPredicate> {

    public BetweenTranslator() {
        super(Between.class, EVPredicate.class);
    }

    @Override
    protected EVPredicate process(Between in, ExpressionTranslator translator, EventSchema schema, Bindings bindings)
            throws TranslatorException, IncompatibleTypeException, SchemaException {
        ArithmeticExpression v = in.getInput(0);
        ArithmeticExpression l = in.getInput(1);
        ArithmeticExpression u = in.getInput(2);

        DataType vt = l.getDataType(schema, bindings);
        DataType lt = l.getDataType(schema, bindings);
        DataType ut = u.getDataType(schema, bindings);

        DataType gct = DataType.getGCT(vt, DataType.getGCT(lt, ut));

        if (gct.isNumeric()) {
            EVNumericExpression tv = translator.translateNumericExpression(v, schema, bindings);
            EVNumericExpression tl = translator.translateNumericExpression(l, schema, bindings);
            EVNumericExpression tu = translator.translateNumericExpression(u, schema, bindings);
            return new EVNumericBetween(gct, tv, tl, tu);
        } else if (gct == DataType.STRING) {
            EVStringExpression tv = translator.translateStringExpression(v, schema, bindings);
            EVStringExpression tl = translator.translateStringExpression(l, schema, bindings);
            EVStringExpression tu = translator.translateStringExpression(u, schema, bindings);
            return new EVStringBetween(tv, tl, tu);
        } else
            throw new IncompatibleTypeException("No implementation for Between with input-types: " + vt + ", " + lt + ", " + ut);
    }
}
