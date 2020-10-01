package sigmod2021.esp.ql.impl.spatial.numeric;

import org.kohsuke.MetaInfServices;
import sigmod2021.common.IncompatibleTypeException;
import sigmod2021.esp.api.epa.pattern.symbol.Bindings;
import sigmod2021.esp.api.expression.spatial.numeric.NumGeometries;
import sigmod2021.esp.expressions.spatial.numeric.EVSpatialNumGeometries;
import sigmod2021.esp.ql.ExpressionTranslator;
import sigmod2021.esp.ql.Translator;
import sigmod2021.esp.ql.TranslatorException;
import sigmod2021.event.EventSchema;
import sigmod2021.event.SchemaException;

/**
 *
 */
@MetaInfServices
public class NumGeometriesTranslator extends Translator<NumGeometries, EVSpatialNumGeometries> {

    public NumGeometriesTranslator() {
        super(NumGeometries.class, EVSpatialNumGeometries.class);
    }

    @Override
    protected EVSpatialNumGeometries process(NumGeometries in, ExpressionTranslator translator, EventSchema schema, Bindings bindings)
            throws TranslatorException, IncompatibleTypeException, SchemaException {
        return new EVSpatialNumGeometries(translator.translateSpatialExpression(in.getInput(0), schema, bindings));
    }

}

