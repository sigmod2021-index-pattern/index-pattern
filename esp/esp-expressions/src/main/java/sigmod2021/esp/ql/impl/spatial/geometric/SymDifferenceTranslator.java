package sigmod2021.esp.ql.impl.spatial.geometric;

import org.kohsuke.MetaInfServices;
import sigmod2021.common.IncompatibleTypeException;
import sigmod2021.esp.api.epa.pattern.symbol.Bindings;
import sigmod2021.esp.api.expression.spatial.geometric.SymDifference;
import sigmod2021.esp.expressions.spatial.geometric.EVSpatialSymDifference;
import sigmod2021.esp.ql.ExpressionTranslator;
import sigmod2021.esp.ql.Translator;
import sigmod2021.esp.ql.TranslatorException;
import sigmod2021.event.EventSchema;
import sigmod2021.event.SchemaException;

/**
 *
 */
@MetaInfServices
public class SymDifferenceTranslator extends Translator<SymDifference, EVSpatialSymDifference> {

    public SymDifferenceTranslator() {
        super(SymDifference.class, EVSpatialSymDifference.class);
    }

    @Override
    protected EVSpatialSymDifference process(SymDifference in, ExpressionTranslator translator, EventSchema schema, Bindings bindings)
            throws TranslatorException, IncompatibleTypeException, SchemaException {
        return new EVSpatialSymDifference(
                translator.translateSpatialExpression(in.getInput(0), schema, bindings),
                translator.translateSpatialExpression(in.getInput(1), schema, bindings));
    }

}
