package sigmod2021.esp.ql.impl.spatial.numeric;

import org.kohsuke.MetaInfServices;
import sigmod2021.common.IncompatibleTypeException;
import sigmod2021.esp.api.epa.pattern.symbol.Bindings;
import sigmod2021.esp.api.expression.spatial.numeric.Dimension;
import sigmod2021.esp.expressions.spatial.numeric.EVSpatialDimension;
import sigmod2021.esp.ql.ExpressionTranslator;
import sigmod2021.esp.ql.Translator;
import sigmod2021.esp.ql.TranslatorException;
import sigmod2021.event.EventSchema;
import sigmod2021.event.SchemaException;

/**
 *
 */
@MetaInfServices
public class DimensionTranslator extends Translator<Dimension, EVSpatialDimension> {

    public DimensionTranslator() {
        super(Dimension.class, EVSpatialDimension.class);
    }

    @Override
    protected EVSpatialDimension process(Dimension in, ExpressionTranslator translator, EventSchema schema, Bindings bindings)
            throws TranslatorException, IncompatibleTypeException, SchemaException {
        return new EVSpatialDimension(translator.translateSpatialExpression(in.getInput(0), schema, bindings));
    }

}

