package sigmod2021.esp.ql.impl.predicate;

import org.kohsuke.MetaInfServices;
import sigmod2021.common.IncompatibleTypeException;
import sigmod2021.esp.api.epa.pattern.symbol.Bindings;
import sigmod2021.esp.api.expression.ArithmeticExpression;
import sigmod2021.esp.api.expression.predicate.spatial.IsRectangle;
import sigmod2021.esp.expressions.arithmetic.EVSpatialExpression;
import sigmod2021.esp.expressions.predicate.EVPredicate;
import sigmod2021.esp.expressions.predicate.spatial.EVSpatialIsRectangle;
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
public class IsRectangleTranslator extends Translator<IsRectangle, EVPredicate> {

    public IsRectangleTranslator() {
        super(IsRectangle.class, EVPredicate.class);
    }

    @Override
    protected EVPredicate process(IsRectangle in, ExpressionTranslator translator, EventSchema schema, Bindings bindings)
            throws TranslatorException, IncompatibleTypeException, SchemaException {
        ArithmeticExpression l = in.getInput(0);

        DataType lt = l.getDataType(schema, bindings);

        if (lt == DataType.GEOMETRY) {
            EVSpatialExpression tl = translator.translateSpatialExpression(l, schema, bindings);
            return new EVSpatialIsRectangle(tl);
        } else
            throw new IncompatibleTypeException("No implementation for isRectangle with input-type: " + lt);
    }
}
