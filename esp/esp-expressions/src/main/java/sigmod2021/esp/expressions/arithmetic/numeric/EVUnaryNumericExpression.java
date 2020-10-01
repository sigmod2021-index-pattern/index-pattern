package sigmod2021.esp.expressions.arithmetic.numeric;

import sigmod2021.esp.expressions.EvaluableExpression;
import sigmod2021.esp.expressions.arithmetic.EVNumericExpression;
import sigmod2021.esp.expressions.util.EVAbstractUnaryExpression;
import sigmod2021.event.Attribute.DataType;

public abstract class EVUnaryNumericExpression extends EVAbstractUnaryExpression<Number, Number>
        implements EVNumericExpression {

    private final DataType type;

    public EVUnaryNumericExpression(EvaluableExpression<Number> input, DataType type, OperatorImpl<Number, Number> impl) {
        super(input, impl);
        this.type = type;
    }

    @Override
    public DataType getType() {
        return type;
    }
}
