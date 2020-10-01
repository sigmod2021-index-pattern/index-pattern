package sigmod2021.esp.expressions.arithmetic.numeric;

import sigmod2021.esp.expressions.EvaluableExpression;
import sigmod2021.esp.expressions.arithmetic.EVNumericExpression;
import sigmod2021.esp.expressions.util.EVAbstractBinaryExpression;
import sigmod2021.event.Attribute.DataType;

public abstract class EVBinaryNumericExpression extends EVAbstractBinaryExpression<Number, Number, Number>
        implements EVNumericExpression {

    private final DataType type;

    public EVBinaryNumericExpression(EvaluableExpression<Number> left, EvaluableExpression<Number> right, DataType type, OperatorImpl<Number, Number, Number> impl) {
        super(left, right, impl);
        this.type = type;
    }

    @Override
    public DataType getType() {
        return type;
    }

}
