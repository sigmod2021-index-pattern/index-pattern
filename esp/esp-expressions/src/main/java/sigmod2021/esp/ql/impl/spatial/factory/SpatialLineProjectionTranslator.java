package sigmod2021.esp.ql.impl.spatial.factory;

import org.kohsuke.MetaInfServices;
import sigmod2021.common.IncompatibleTypeException;
import sigmod2021.esp.api.epa.pattern.symbol.Bindings;
import sigmod2021.esp.api.expression.spatial.factory.SpatialLineProjection;
import sigmod2021.esp.expressions.arithmetic.EVSpatialExpression;
import sigmod2021.esp.expressions.spatial.factory.EVSpatialLineProjection;
import sigmod2021.esp.ql.ExpressionTranslator;
import sigmod2021.esp.ql.Translator;
import sigmod2021.esp.ql.TranslatorException;
import sigmod2021.event.EventSchema;
import sigmod2021.event.SchemaException;

import java.util.ArrayList;
import java.util.List;

@MetaInfServices
public class SpatialLineProjectionTranslator extends Translator<SpatialLineProjection, EVSpatialLineProjection> {

    public SpatialLineProjectionTranslator() {
        super(SpatialLineProjection.class, EVSpatialLineProjection.class);
    }

    @Override
    protected EVSpatialLineProjection process(SpatialLineProjection in, ExpressionTranslator translator, EventSchema schema, Bindings bindings) throws TranslatorException, IncompatibleTypeException, SchemaException {
        List<EVSpatialExpression> input = new ArrayList<>();
        for (int i = 0; i < in.getArity(); i++) {
            input.add(translator.translateSpatialExpression(in.getInput(i), schema, bindings));
        }
        return new EVSpatialLineProjection(input);
    }

}
