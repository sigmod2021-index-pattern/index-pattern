package sigmod2021.esp.ql.impl.atomic;

import org.kohsuke.MetaInfServices;
import sigmod2021.esp.api.epa.pattern.symbol.Bindings;
import sigmod2021.esp.api.expression.arithmetic.atomic.GeoConstant;
import sigmod2021.esp.expressions.arithmetic.constant.EVSpatialConstant;
import sigmod2021.esp.ql.ExpressionTranslator;
import sigmod2021.esp.ql.Translator;
import sigmod2021.esp.ql.TranslatorException;
import sigmod2021.event.EventSchema;

/**
 *
 */
@MetaInfServices
public class SpatialConstantTranslator extends Translator<GeoConstant, EVSpatialConstant> {

    public SpatialConstantTranslator() {
        super(GeoConstant.class, EVSpatialConstant.class);
    }

    @Override
    protected EVSpatialConstant process(GeoConstant in, ExpressionTranslator translator, EventSchema schema, Bindings bindings)
            throws TranslatorException {
        return new EVSpatialConstant(in.getValue());
    }

}
