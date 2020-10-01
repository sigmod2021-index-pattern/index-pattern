package sigmod2021.esp.ql.impl.spatial.geometric;

import org.kohsuke.MetaInfServices;
import sigmod2021.common.IncompatibleTypeException;
import sigmod2021.esp.api.epa.pattern.symbol.Bindings;
import sigmod2021.esp.api.expression.spatial.geometric.Difference;
import sigmod2021.esp.expressions.spatial.geometric.EVSpatialDifference;
import sigmod2021.esp.ql.ExpressionTranslator;
import sigmod2021.esp.ql.Translator;
import sigmod2021.esp.ql.TranslatorException;
import sigmod2021.event.EventSchema;
import sigmod2021.event.SchemaException;

/**
 *
 */
@MetaInfServices
public class DifferenceTranslator extends Translator<Difference, EVSpatialDifference> {

    public DifferenceTranslator() {
        super(Difference.class, EVSpatialDifference.class);
    }

    @Override
    protected EVSpatialDifference process(Difference in, ExpressionTranslator translator, EventSchema schema, Bindings bindings)
            throws TranslatorException, IncompatibleTypeException, SchemaException {
        return new EVSpatialDifference(
                translator.translateSpatialExpression(in.getInput(0), schema, bindings),
                translator.translateSpatialExpression(in.getInput(1), schema, bindings));
    }

}
