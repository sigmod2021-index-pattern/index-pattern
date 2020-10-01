package sigmod2021.esp.ql.impl.spatial.geometric;

import org.kohsuke.MetaInfServices;
import sigmod2021.common.IncompatibleTypeException;
import sigmod2021.esp.api.epa.pattern.symbol.Bindings;
import sigmod2021.esp.api.expression.spatial.geometric.Intersection;
import sigmod2021.esp.expressions.spatial.geometric.EVSpatialIntersection;
import sigmod2021.esp.ql.ExpressionTranslator;
import sigmod2021.esp.ql.Translator;
import sigmod2021.esp.ql.TranslatorException;
import sigmod2021.event.EventSchema;
import sigmod2021.event.SchemaException;

/**
 *
 */
@MetaInfServices
public class IntersectionTranslator extends Translator<Intersection, EVSpatialIntersection> {

    public IntersectionTranslator() {
        super(Intersection.class, EVSpatialIntersection.class);
    }

    @Override
    protected EVSpatialIntersection process(Intersection in, ExpressionTranslator translator, EventSchema schema, Bindings bindings)
            throws TranslatorException, IncompatibleTypeException, SchemaException {
        return new EVSpatialIntersection(
                translator.translateSpatialExpression(in.getInput(0), schema, bindings),
                translator.translateSpatialExpression(in.getInput(1), schema, bindings));
    }

}
