package sigmod2021.esp.api.expression.arithmetic.atomic;

import sigmod2021.event.Attribute.DataType;

/**
 * A constant double value
 */
public class DoubleConstant extends Constant<Double> {

    /**
     * @param value The value
     */
    public DoubleConstant(Double value) {
        super(value, DataType.DOUBLE);
    }
}
