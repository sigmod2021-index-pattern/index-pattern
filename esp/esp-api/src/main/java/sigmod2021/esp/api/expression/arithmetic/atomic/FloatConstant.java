package sigmod2021.esp.api.expression.arithmetic.atomic;

import sigmod2021.event.Attribute.DataType;

/**
 * A constant float value
 */
public class FloatConstant extends Constant<Float> {

    /**
     * @param value The value
     */
    public FloatConstant(Float value) {
        super(value, DataType.FLOAT);
    }
}
