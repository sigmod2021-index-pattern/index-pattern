package sigmod2021.esp.ql.impl.spatial.numeric;

import org.kohsuke.MetaInfServices;
import sigmod2021.common.IncompatibleTypeException;
import sigmod2021.esp.api.epa.pattern.symbol.Bindings;
import sigmod2021.esp.api.expression.spatial.numeric.Distance;
import sigmod2021.esp.expressions.spatial.numeric.EVSpatialDistance;
import sigmod2021.esp.ql.ExpressionTranslator;
import sigmod2021.esp.ql.Translator;
import sigmod2021.esp.ql.TranslatorException;
import sigmod2021.event.EventSchema;
import sigmod2021.event.SchemaException;

/**
 *
 */
@MetaInfServices
public class DistanceTranslator extends Translator<Distance, EVSpatialDistance> {

    public DistanceTranslator() {
        super(Distance.class, EVSpatialDistance.class);
    }

    @Override
    protected EVSpatialDistance process(Distance in, ExpressionTranslator translator, EventSchema schema, Bindings bindings)
            throws TranslatorException, IncompatibleTypeException, SchemaException {
        return new EVSpatialDistance(
                translator.translateSpatialExpression(in.getInput(0), schema, bindings),
                translator.translateSpatialExpression(in.getInput(1), schema, bindings));
    }

}
