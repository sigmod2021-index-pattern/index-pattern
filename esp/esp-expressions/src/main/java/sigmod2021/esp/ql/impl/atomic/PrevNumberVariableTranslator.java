package sigmod2021.esp.ql.impl.atomic;

import org.kohsuke.MetaInfServices;
import sigmod2021.common.IncompatibleTypeException;
import sigmod2021.esp.api.epa.pattern.symbol.Bindings;
import sigmod2021.esp.api.expression.arithmetic.atomic.PrevVariable;
import sigmod2021.esp.expressions.arithmetic.field.*;
import sigmod2021.esp.ql.ExpressionTranslator;
import sigmod2021.esp.ql.Translator;
import sigmod2021.esp.ql.TranslatorException;
import sigmod2021.event.Attribute;
import sigmod2021.event.EventSchema;
import sigmod2021.event.SchemaException;

@MetaInfServices
public class PrevNumberVariableTranslator extends Translator<PrevVariable, EVNumericPrevVariable> {

    public PrevNumberVariableTranslator() {
        super(PrevVariable.class, EVNumericPrevVariable.class);
    }

    @Override
    protected EVNumericPrevVariable process(PrevVariable in, ExpressionTranslator translator, EventSchema schema, Bindings bindings) throws TranslatorException, IncompatibleTypeException, SchemaException {
        int index = schema.getAttributeIndex(in.getName());
        Attribute.DataType type = schema.getAttribute(index).getType();
        boolean isBinding = false;
        int prev = in.getPrev();
        EVVariable<Number> variable = null;

        switch (type) {
            case BYTE:
                variable = new EVByteVariable(index, isBinding);
                break;
            case SHORT:
                variable = new EVShortVariable(index, isBinding);
                break;
            case INTEGER:
                variable = new EVIntVariable(index, isBinding);
                break;
            case LONG:
                variable = new EVLongVariable(index, isBinding);
                break;
            case FLOAT:
                variable = new EVFloatVariable(index, isBinding);
                break;
            case DOUBLE:
                variable = new EVDoubleVariable(index, isBinding);
                break;
            default:
                throw new TranslatorException("Not a numeric attribute: " + in.getName());
        }

        return new EVNumericPrevVariable(index, type, prev, variable);
    }
}
