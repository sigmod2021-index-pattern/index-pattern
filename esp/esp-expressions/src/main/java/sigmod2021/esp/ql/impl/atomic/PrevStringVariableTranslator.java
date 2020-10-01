package sigmod2021.esp.ql.impl.atomic;

import org.kohsuke.MetaInfServices;
import sigmod2021.common.IncompatibleTypeException;
import sigmod2021.esp.api.epa.pattern.symbol.Bindings;
import sigmod2021.esp.api.expression.arithmetic.atomic.PrevVariable;
import sigmod2021.esp.expressions.arithmetic.field.EVStringPrevVariable;
import sigmod2021.esp.expressions.arithmetic.field.EVStringVariable;
import sigmod2021.esp.expressions.arithmetic.field.EVVariable;
import sigmod2021.esp.ql.ExpressionTranslator;
import sigmod2021.esp.ql.Translator;
import sigmod2021.esp.ql.TranslatorException;
import sigmod2021.event.Attribute;
import sigmod2021.event.EventSchema;
import sigmod2021.event.SchemaException;

@MetaInfServices
public class PrevStringVariableTranslator extends Translator<PrevVariable, EVStringPrevVariable> {

    public PrevStringVariableTranslator() {
        super(PrevVariable.class, EVStringPrevVariable.class);
    }

    @Override
    protected EVStringPrevVariable process(PrevVariable in, ExpressionTranslator translator, EventSchema schema, Bindings bindings) throws TranslatorException, IncompatibleTypeException, SchemaException {
        int index = schema.getAttributeIndex(in.getName());
        Attribute.DataType type = schema.getAttribute(index).getType();
        boolean isBinding = false;
        int prev = in.getPrev();
        EVVariable<String> variable = null;

        if (type == Attribute.DataType.STRING)
            variable = new EVStringVariable(index, isBinding);
        else
            throw new TranslatorException("Not a string-variable: " + in.getName());

        return new EVStringPrevVariable(index, type, prev, variable);
    }
}
