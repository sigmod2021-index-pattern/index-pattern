package sigmod2021.esp.bindings;

import sigmod2021.common.IncompatibleTypeException;
import sigmod2021.esp.api.epa.pattern.symbol.Binding;
import sigmod2021.esp.api.epa.pattern.symbol.Bindings;
import sigmod2021.esp.expressions.arithmetic.EVArithmeticExpression;
import sigmod2021.esp.ql.ExpressionTranslator;
import sigmod2021.esp.ql.TranslatorException;
import sigmod2021.event.Attribute.DataType;
import sigmod2021.event.EventSchema;
import sigmod2021.event.SchemaException;

public class EVBinding extends Binding {

    private final EVArithmeticExpression<?> expression;

    public EVBinding(Binding b, EventSchema schema, Bindings bindings) throws TranslatorException, IncompatibleTypeException, SchemaException {
        super(b.getName(), b.getValue(), b.getBindingTime());
        DataType dt = b.getValue().getDataType(schema, bindings);

        if (dt.isNumeric()) {
            this.expression = ExpressionTranslator.INSTANCE.translateNumericExpression(b.getValue(), schema, bindings);
        } else if (dt == DataType.STRING) {
            this.expression = ExpressionTranslator.INSTANCE.translateStringExpression(b.getValue(), schema, bindings);
        } else if (dt == DataType.SET) {
            this.expression = ExpressionTranslator.INSTANCE.translateSetExpression(b.getValue(), schema, bindings);
        } else if (dt == DataType.GEOMETRY) {
            this.expression = ExpressionTranslator.INSTANCE.translateSpatialExpression(b.getValue(), schema, bindings);
        } else
            throw new IncompatibleTypeException("Type " + dt + " not supported in VariableBindings");
    }

    public EVArithmeticExpression<?> getExpression() {
        return expression;
    }

}
