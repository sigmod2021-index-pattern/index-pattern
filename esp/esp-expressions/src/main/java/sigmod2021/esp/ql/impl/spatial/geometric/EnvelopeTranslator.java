package sigmod2021.esp.ql.impl.spatial.geometric;

import org.kohsuke.MetaInfServices;
import sigmod2021.common.IncompatibleTypeException;
import sigmod2021.esp.api.epa.pattern.symbol.Bindings;
import sigmod2021.esp.api.expression.spatial.geometric.Envelope;
import sigmod2021.esp.expressions.spatial.geometric.EVSpatialEnvelope;
import sigmod2021.esp.ql.ExpressionTranslator;
import sigmod2021.esp.ql.Translator;
import sigmod2021.esp.ql.TranslatorException;
import sigmod2021.event.EventSchema;
import sigmod2021.event.SchemaException;

/**
 *
 */
@MetaInfServices
public class EnvelopeTranslator extends Translator<Envelope, EVSpatialEnvelope> {

    public EnvelopeTranslator() {
        super(Envelope.class, EVSpatialEnvelope.class);
    }

    @Override
    protected EVSpatialEnvelope process(Envelope in, ExpressionTranslator translator, EventSchema schema, Bindings bindings)
            throws TranslatorException, IncompatibleTypeException, SchemaException {
        return new EVSpatialEnvelope(translator.translateSpatialExpression(in.getInput(0), schema, bindings));
    }

}
