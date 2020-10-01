package sigmod2021.esp.ql.impl.spatial.geometric;

import org.kohsuke.MetaInfServices;
import sigmod2021.common.IncompatibleTypeException;
import sigmod2021.esp.api.epa.pattern.symbol.Bindings;
import sigmod2021.esp.api.expression.spatial.geometric.Buffer;
import sigmod2021.esp.expressions.spatial.geometric.EVSpatialBuffer;
import sigmod2021.esp.ql.ExpressionTranslator;
import sigmod2021.esp.ql.Translator;
import sigmod2021.esp.ql.TranslatorException;
import sigmod2021.event.EventSchema;
import sigmod2021.event.SchemaException;

/**
 *
 */
@MetaInfServices
public class BufferTranslator extends Translator<Buffer, EVSpatialBuffer> {

    public BufferTranslator() {
        super(Buffer.class, EVSpatialBuffer.class);
    }

    @Override
    protected EVSpatialBuffer process(Buffer in, ExpressionTranslator translator, EventSchema schema, Bindings bindings)
            throws TranslatorException, IncompatibleTypeException, SchemaException {
        return new EVSpatialBuffer(translator.translateSpatialExpression(in.getInput(0), schema, bindings), in.getDistance(), in.getCapStyle());
    }

}

