package sigmod2021.esp.ql.impl.atomic;

import org.kohsuke.MetaInfServices;
import sigmod2021.common.IncompatibleTypeException;
import sigmod2021.esp.api.epa.pattern.symbol.Binding;
import sigmod2021.esp.api.epa.pattern.symbol.Bindings;
import sigmod2021.esp.api.expression.arithmetic.atomic.Variable;
import sigmod2021.esp.expressions.arithmetic.field.EVSetVariable;
import sigmod2021.esp.ql.ExpressionTranslator;
import sigmod2021.esp.ql.Translator;
import sigmod2021.esp.ql.TranslatorException;
import sigmod2021.event.Attribute.DataType;
import sigmod2021.event.EventSchema;
import sigmod2021.event.SchemaException;

/**
 *
 */
@MetaInfServices
public class SetVariableTranslator extends Translator<Variable, EVSetVariable> {

    public SetVariableTranslator() {
        super(Variable.class, EVSetVariable.class);
    }

    @Override
    protected EVSetVariable process(Variable in, ExpressionTranslator translator, EventSchema schema, Bindings bindings)
            throws TranslatorException, SchemaException {


        int index = -1;
        DataType type = null;
        boolean isBinding = false;

        try {
            index = schema.getAttributeIndex(in.getName());
            type = schema.getAttribute(index).getType();
        } catch (SchemaException se) {
            Binding b = bindings.byName(in.getName());
            index = bindings.indexOf(b.getName());
            try {
                type = b.getValue().getDataType(schema, bindings.without(b));
            } catch (IncompatibleTypeException e) {
                throw new TranslatorException("Binding has incompatible type", e);
            }
            isBinding = true;
        }

        if (type == DataType.SET)
            return new EVSetVariable(index, isBinding);
        else
            throw new TranslatorException("Not a set-variable: " + in.getName());
    }
}
