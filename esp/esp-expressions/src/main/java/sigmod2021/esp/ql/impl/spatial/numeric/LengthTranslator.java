package sigmod2021.esp.ql.impl.spatial.numeric;

import org.kohsuke.MetaInfServices;
import sigmod2021.common.IncompatibleTypeException;
import sigmod2021.esp.api.epa.pattern.symbol.Bindings;
import sigmod2021.esp.api.expression.spatial.numeric.Length;
import sigmod2021.esp.expressions.spatial.numeric.EVSpatialLength;
import sigmod2021.esp.ql.ExpressionTranslator;
import sigmod2021.esp.ql.Translator;
import sigmod2021.esp.ql.TranslatorException;
import sigmod2021.event.EventSchema;
import sigmod2021.event.SchemaException;

/**
 *
 */
@MetaInfServices
public class LengthTranslator extends Translator<Length, EVSpatialLength> {

    public LengthTranslator() {
        super(Length.class, EVSpatialLength.class);
    }

    @Override
    protected EVSpatialLength process(Length in, ExpressionTranslator translator, EventSchema schema, Bindings bindings)
            throws TranslatorException, IncompatibleTypeException, SchemaException {
        return new EVSpatialLength(translator.translateSpatialExpression(in.getInput(0), schema, bindings));
    }

}
