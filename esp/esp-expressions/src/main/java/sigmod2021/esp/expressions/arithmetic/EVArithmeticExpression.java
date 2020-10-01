package sigmod2021.esp.expressions.arithmetic;

import sigmod2021.esp.expressions.EvaluableExpression;
import sigmod2021.event.Attribute.DataType;

public interface EVArithmeticExpression<T> extends EvaluableExpression<T> {

    DataType getType();

}
