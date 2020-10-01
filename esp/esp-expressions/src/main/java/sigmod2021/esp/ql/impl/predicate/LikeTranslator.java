package sigmod2021.esp.ql.impl.predicate;

import org.kohsuke.MetaInfServices;
import sigmod2021.common.IncompatibleTypeException;
import sigmod2021.esp.api.epa.pattern.symbol.Bindings;
import sigmod2021.esp.api.epa.pattern.symbol.NoSuchVariableException;
import sigmod2021.esp.api.expression.ArithmeticExpression;
import sigmod2021.esp.api.expression.predicate.Like;
import sigmod2021.esp.expressions.arithmetic.EVStringExpression;
import sigmod2021.esp.expressions.predicate.EVPredicate;
import sigmod2021.esp.expressions.predicate.string.EVStringLike;
import sigmod2021.esp.ql.ExpressionTranslator;
import sigmod2021.esp.ql.Translator;
import sigmod2021.esp.ql.TranslatorException;
import sigmod2021.event.Attribute.DataType;
import sigmod2021.event.EventSchema;

/**
 * Translator for Like predicate
 */
@MetaInfServices
public class LikeTranslator extends Translator<Like, EVPredicate> {

    public LikeTranslator() {
        super(Like.class, EVPredicate.class);
    }

    @Override
    protected EVPredicate process(Like in, ExpressionTranslator translator, EventSchema schema, Bindings bindings)
            throws TranslatorException, IncompatibleTypeException, NoSuchVariableException {
        ArithmeticExpression l = in.getInput(0);
        ArithmeticExpression r = in.getInput(1);
        DataType lt = l.getDataType(schema, bindings);
        DataType rt = r.getDataType(schema, bindings);

        DataType gct = DataType.getGCT(lt, rt);

        if (gct == DataType.STRING) {
            EVStringExpression tl = translator.translateStringExpression(l, schema, bindings);
            EVStringExpression tr = translator.translateStringExpression(r, schema, bindings);
            return new EVStringLike(tl, tr);
        } else
            throw new IncompatibleTypeException("No implementation for Equal with input-types: " + lt + ", " + rt);
    }
}

