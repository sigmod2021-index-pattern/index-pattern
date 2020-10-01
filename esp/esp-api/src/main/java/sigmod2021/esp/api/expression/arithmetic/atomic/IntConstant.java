package sigmod2021.esp.api.expression.arithmetic.atomic;

import sigmod2021.event.Attribute.DataType;

/**
 * A constant int value
 */
public class IntConstant extends Constant<Integer> {

    /**
     * @param value The value
     */
    public IntConstant(Integer value) {
        super(value, DataType.INTEGER);
    }

}
