package sigmod2021.esp.ql.impl.spatial.geometric;

import org.kohsuke.MetaInfServices;
import sigmod2021.common.IncompatibleTypeException;
import sigmod2021.esp.api.epa.pattern.symbol.Bindings;
import sigmod2021.esp.api.expression.spatial.geometric.Union;
import sigmod2021.esp.expressions.spatial.geometric.EVSpatialUnion;
import sigmod2021.esp.ql.ExpressionTranslator;
import sigmod2021.esp.ql.Translator;
import sigmod2021.esp.ql.TranslatorException;
import sigmod2021.event.EventSchema;
import sigmod2021.event.SchemaException;

/**
 *
 */
@MetaInfServices
public class UnionTranslator extends Translator<Union, EVSpatialUnion> {

    public UnionTranslator() {
        super(Union.class, EVSpatialUnion.class);
    }

    @Override
    protected EVSpatialUnion process(Union in, ExpressionTranslator translator, EventSchema schema, Bindings bindings)
            throws TranslatorException, IncompatibleTypeException, SchemaException {
        return new EVSpatialUnion(
                translator.translateSpatialExpression(in.getInput(0), schema, bindings),
                translator.translateSpatialExpression(in.getInput(1), schema, bindings));
    }

}
