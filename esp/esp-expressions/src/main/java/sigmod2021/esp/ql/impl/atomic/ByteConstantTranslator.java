package sigmod2021.esp.ql.impl.atomic;

import org.kohsuke.MetaInfServices;
import sigmod2021.esp.api.epa.pattern.symbol.Bindings;
import sigmod2021.esp.api.expression.arithmetic.atomic.ByteConstant;
import sigmod2021.esp.expressions.arithmetic.constant.EVByteConstant;
import sigmod2021.esp.ql.ExpressionTranslator;
import sigmod2021.esp.ql.Translator;
import sigmod2021.esp.ql.TranslatorException;
import sigmod2021.event.EventSchema;

@MetaInfServices
public class ByteConstantTranslator extends Translator<ByteConstant, EVByteConstant> {

    public ByteConstantTranslator() {
        super(ByteConstant.class, EVByteConstant.class);
    }

    @Override
    protected EVByteConstant process(ByteConstant in, ExpressionTranslator translator, EventSchema schema, Bindings bindings)
            throws TranslatorException {
        return new EVByteConstant(in.getValue());
    }

}
