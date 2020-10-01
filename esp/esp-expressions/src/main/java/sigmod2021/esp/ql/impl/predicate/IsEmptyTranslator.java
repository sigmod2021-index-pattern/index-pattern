package sigmod2021.esp.ql.impl.predicate;

import org.kohsuke.MetaInfServices;
import sigmod2021.common.IncompatibleTypeException;
import sigmod2021.esp.api.epa.pattern.symbol.Bindings;
import sigmod2021.esp.api.expression.ArithmeticExpression;
import sigmod2021.esp.api.expression.predicate.spatial.IsEmpty;
import sigmod2021.esp.expressions.arithmetic.EVSpatialExpression;
import sigmod2021.esp.expressions.predicate.EVPredicate;
import sigmod2021.esp.expressions.predicate.spatial.EVSpatialIsEmpty;
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
public class IsEmptyTranslator extends Translator<IsEmpty, EVPredicate> {

    public IsEmptyTranslator() {
        super(IsEmpty.class, EVPredicate.class);
    }

    @Override
    protected EVPredicate process(IsEmpty in, ExpressionTranslator translator, EventSchema schema, Bindings bindings)
            throws TranslatorException, IncompatibleTypeException, SchemaException {
        ArithmeticExpression l = in.getInput(0);

        DataType lt = l.getDataType(schema, bindings);

        if (lt == DataType.GEOMETRY) {
            EVSpatialExpression tl = translator.translateSpatialExpression(l, schema, bindings);
            return new EVSpatialIsEmpty(tl);
        } else
            throw new IncompatibleTypeException("No implementation for isEmpty with input-type: " + lt);
    }
}

