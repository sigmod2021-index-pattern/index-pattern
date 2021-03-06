package sigmod2021.esp.ql.impl.arithmetic.numeric;

import org.kohsuke.MetaInfServices;
import sigmod2021.common.IncompatibleTypeException;
import sigmod2021.esp.api.epa.pattern.symbol.Bindings;
import sigmod2021.esp.api.expression.arithmetic.compund.numeric.ASin;
import sigmod2021.esp.expressions.arithmetic.numeric.EVASin;
import sigmod2021.esp.ql.ExpressionTranslator;
import sigmod2021.esp.ql.Translator;
import sigmod2021.esp.ql.TranslatorException;
import sigmod2021.event.EventSchema;
import sigmod2021.event.SchemaException;

@MetaInfServices
public class ASinTranslator extends Translator<ASin, EVASin> {

    public ASinTranslator() {
        super(ASin.class, EVASin.class);
    }

    @Override
    protected EVASin process(ASin in, ExpressionTranslator translator, EventSchema schema, Bindings bindings)
            throws TranslatorException, IncompatibleTypeException, SchemaException {
        return new EVASin(translator.translateNumericExpression(in.getInput(0), schema, bindings));
    }

}
