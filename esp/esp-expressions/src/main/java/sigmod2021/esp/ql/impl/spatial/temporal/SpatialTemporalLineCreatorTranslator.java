package sigmod2021.esp.ql.impl.spatial.temporal;

import org.kohsuke.MetaInfServices;
import sigmod2021.common.IncompatibleTypeException;
import sigmod2021.esp.api.epa.pattern.symbol.Bindings;
import sigmod2021.esp.api.epa.pattern.symbol.NoSuchVariableException;
import sigmod2021.esp.api.expression.spatial.temporal.SpatialTemporalLineCreator;
import sigmod2021.esp.expressions.spatial.temporal.EVSpatialTemporalLineCreator;
import sigmod2021.esp.ql.ExpressionTranslator;
import sigmod2021.esp.ql.Translator;
import sigmod2021.esp.ql.TranslatorException;
import sigmod2021.event.EventSchema;
import sigmod2021.event.SchemaException;

@MetaInfServices
public class SpatialTemporalLineCreatorTranslator extends Translator<SpatialTemporalLineCreator, EVSpatialTemporalLineCreator> {

    public SpatialTemporalLineCreatorTranslator() {
        super(SpatialTemporalLineCreator.class, EVSpatialTemporalLineCreator.class);
    }

    @Override
    protected EVSpatialTemporalLineCreator process(SpatialTemporalLineCreator in, ExpressionTranslator translator, EventSchema schema, Bindings bindings) throws TranslatorException, IncompatibleTypeException, NoSuchVariableException, SchemaException {
        return new EVSpatialTemporalLineCreator(translator.translateSpatialExpression(in.getInput(0), schema, bindings));
    }

}
