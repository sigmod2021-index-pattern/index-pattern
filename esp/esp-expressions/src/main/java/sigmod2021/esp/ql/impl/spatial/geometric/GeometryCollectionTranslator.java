package sigmod2021.esp.ql.impl.spatial.geometric;

import org.kohsuke.MetaInfServices;
import sigmod2021.common.IncompatibleTypeException;
import sigmod2021.esp.api.epa.pattern.symbol.Bindings;
import sigmod2021.esp.api.expression.spatial.geometric.GeometryCollection;
import sigmod2021.esp.expressions.arithmetic.EVSpatialExpression;
import sigmod2021.esp.expressions.spatial.geometric.EVSpatialGeometryCollection;
import sigmod2021.esp.ql.ExpressionTranslator;
import sigmod2021.esp.ql.Translator;
import sigmod2021.esp.ql.TranslatorException;
import sigmod2021.event.EventSchema;
import sigmod2021.event.SchemaException;

/**
 *
 */
@MetaInfServices
public class GeometryCollectionTranslator extends Translator<GeometryCollection, EVSpatialGeometryCollection> {

    public GeometryCollectionTranslator() {
        super(GeometryCollection.class, EVSpatialGeometryCollection.class);
    }

    @Override
    protected EVSpatialGeometryCollection process(GeometryCollection in, ExpressionTranslator translator, EventSchema schema, Bindings bindings)
            throws TranslatorException, IncompatibleTypeException, SchemaException {
        EVSpatialExpression[] translated = new EVSpatialExpression[in.getArity()];
        for (int i = 0; i < in.getArity(); i++) {
            translated[i] = translator.translateSpatialExpression(in.getInput(i), schema, bindings);
        }
        return new EVSpatialGeometryCollection(translated);
    }

}
