package sigmod2021.esp.ql.impl.spatial.geometric;

import org.kohsuke.MetaInfServices;
import sigmod2021.common.IncompatibleTypeException;
import sigmod2021.esp.api.epa.pattern.symbol.Bindings;
import sigmod2021.esp.api.expression.spatial.geometric.GeometryN;
import sigmod2021.esp.expressions.spatial.geometric.EVSpatialGeometryN;
import sigmod2021.esp.ql.ExpressionTranslator;
import sigmod2021.esp.ql.Translator;
import sigmod2021.esp.ql.TranslatorException;
import sigmod2021.event.EventSchema;
import sigmod2021.event.SchemaException;

/**
 *
 */
@MetaInfServices
public class GeometryNTranslator extends Translator<GeometryN, EVSpatialGeometryN> {

    public GeometryNTranslator() {
        super(GeometryN.class, EVSpatialGeometryN.class);
    }

    @Override
    protected EVSpatialGeometryN process(GeometryN in, ExpressionTranslator translator, EventSchema schema, Bindings bindings)
            throws TranslatorException, IncompatibleTypeException, SchemaException {
        return new EVSpatialGeometryN(translator.translateSpatialExpression(in.getInput(0), schema, bindings), in.getN());
    }

}
