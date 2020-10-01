package sigmod2021.esp.ql.impl.spatial.numeric;

import org.kohsuke.MetaInfServices;
import sigmod2021.common.IncompatibleTypeException;
import sigmod2021.esp.api.epa.pattern.symbol.Bindings;
import sigmod2021.esp.api.expression.spatial.numeric.NumPoints;
import sigmod2021.esp.expressions.spatial.numeric.EVSpatialNumPoints;
import sigmod2021.esp.ql.ExpressionTranslator;
import sigmod2021.esp.ql.Translator;
import sigmod2021.esp.ql.TranslatorException;
import sigmod2021.event.EventSchema;
import sigmod2021.event.SchemaException;

/**
 *
 */
@MetaInfServices
public class NumPointsTranslator extends Translator<NumPoints, EVSpatialNumPoints> {

    public NumPointsTranslator() {
        super(NumPoints.class, EVSpatialNumPoints.class);
    }

    @Override
    protected EVSpatialNumPoints process(NumPoints in, ExpressionTranslator translator, EventSchema schema, Bindings bindings)
            throws TranslatorException, IncompatibleTypeException, SchemaException {
        return new EVSpatialNumPoints(translator.translateSpatialExpression(in.getInput(0), schema, bindings));
    }

}
