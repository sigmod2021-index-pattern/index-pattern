package sigmod2021.esp.ql.impl.spatial.geometric;

import org.kohsuke.MetaInfServices;
import sigmod2021.common.IncompatibleTypeException;
import sigmod2021.esp.api.epa.pattern.symbol.Bindings;
import sigmod2021.esp.api.expression.spatial.geometric.ConvexHull;
import sigmod2021.esp.expressions.spatial.geometric.EVSpatialConvexHull;
import sigmod2021.esp.ql.ExpressionTranslator;
import sigmod2021.esp.ql.Translator;
import sigmod2021.esp.ql.TranslatorException;
import sigmod2021.event.EventSchema;
import sigmod2021.event.SchemaException;

/**
 *
 */
@MetaInfServices
public class ConvexHullTranslator extends Translator<ConvexHull, EVSpatialConvexHull> {

    public ConvexHullTranslator() {
        super(ConvexHull.class, EVSpatialConvexHull.class);
    }

    @Override
    protected EVSpatialConvexHull process(ConvexHull in, ExpressionTranslator translator, EventSchema schema, Bindings bindings)
            throws TranslatorException, IncompatibleTypeException, SchemaException {
        return new EVSpatialConvexHull(translator.translateSpatialExpression(in.getInput(0), schema, bindings));
    }

}
