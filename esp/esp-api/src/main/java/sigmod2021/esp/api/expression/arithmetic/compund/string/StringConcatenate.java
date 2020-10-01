package sigmod2021.esp.api.expression.arithmetic.compund.string;

import sigmod2021.common.IncompatibleTypeException;
import sigmod2021.esp.api.epa.pattern.symbol.Bindings;
import sigmod2021.esp.api.expression.ArithmeticExpression;
import sigmod2021.esp.api.expression.base.AbstractExpression;
import sigmod2021.event.Attribute.DataType;
import sigmod2021.event.EventSchema;
import sigmod2021.event.SchemaException;

/**
 *
 */
public class StringConcatenate extends AbstractExpression<ArithmeticExpression> implements ArithmeticExpression {

    /**
     * @param left the left input expression
     * @param right the right input expression
     */
    public StringConcatenate(ArithmeticExpression left, ArithmeticExpression right) {
        super(left, right);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String toString() {
        return "" + getInput(0) + " " + getInput(1);
    }


    @Override
    public DataType getDataType(EventSchema schema, Bindings bindings)
            throws IncompatibleTypeException, SchemaException {
        return DataType.STRING;
    }

}

