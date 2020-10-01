package sigmod2021.esp.ql.impl.predicate;

import org.kohsuke.MetaInfServices;
import sigmod2021.common.IncompatibleTypeException;
import sigmod2021.esp.api.epa.pattern.symbol.Bindings;
import sigmod2021.esp.api.expression.ArithmeticExpression;
import sigmod2021.esp.api.expression.predicate.spatial.Disjoint;
import sigmod2021.esp.expressions.arithmetic.EVSpatialExpression;
import sigmod2021.esp.expressions.predicate.EVPredicate;
import sigmod2021.esp.expressions.predicate.spatial.EVSpatialDisjoint;
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
public class DisjointTranslator extends Translator<Disjoint, EVPredicate> {

    public DisjointTranslator() {
        super(Disjoint.class, EVPredicate.class);
    }

    @Override
    protected EVPredicate process(Disjoint in, ExpressionTranslator translator, EventSchema schema, Bindings bindings)
            throws TranslatorException, IncompatibleTypeException, SchemaException {
        ArithmeticExpression l = in.getInput(0);
        ArithmeticExpression r = in.getInput(1);

        DataType lt = l.getDataType(schema, bindings);
        DataType rt = r.getDataType(schema, bindings);

        DataType gct = DataType.getGCT(lt, rt);

        if (gct == DataType.GEOMETRY) {
            EVSpatialExpression tl = translator.translateSpatialExpression(l, schema, bindings);
            EVSpatialExpression tr = translator.translateSpatialExpression(r, schema, bindings);
            return new EVSpatialDisjoint(tl, tr);
        } else
            throw new IncompatibleTypeException("No implementation for Disjoint with input-types: " + lt + " and " + rt);
    }
}

