package sigmod2021.esp.ql.impl.arithmetic.numeric;

import org.kohsuke.MetaInfServices;
import sigmod2021.common.IncompatibleTypeException;
import sigmod2021.esp.api.epa.pattern.symbol.Bindings;
import sigmod2021.esp.api.expression.arithmetic.compund.numeric.ATan;
import sigmod2021.esp.expressions.arithmetic.numeric.EVATan;
import sigmod2021.esp.ql.ExpressionTranslator;
import sigmod2021.esp.ql.Translator;
import sigmod2021.esp.ql.TranslatorException;
import sigmod2021.event.EventSchema;
import sigmod2021.event.SchemaException;

@MetaInfServices
public class ATanTranslator extends Translator<ATan, EVATan> {

    public ATanTranslator() {
        super(ATan.class, EVATan.class);
    }

    @Override
    protected EVATan process(ATan in, ExpressionTranslator translator, EventSchema schema, Bindings bindings)
            throws TranslatorException, IncompatibleTypeException, SchemaException {
        return new EVATan(translator.translateNumericExpression(in.getInput(0), schema, bindings));
    }

}
