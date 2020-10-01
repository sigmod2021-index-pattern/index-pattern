package sigmod2021.esp.api.expression.arithmetic.atomic;

import sigmod2021.event.Attribute.DataType;

/**
 * A constant byte value
 */
public class ShortConstant extends Constant<Short> {

    /**
     * @param value The value
     */
    public ShortConstant(Short value) {
        super(value, DataType.SHORT);
    }

    public ShortConstant(Integer value) {
        this(convert(value));
    }

    private static Short convert(Integer value) {
        if (value > Short.MAX_VALUE || value < Short.MIN_VALUE) {
            throw new IllegalArgumentException("Value out of range for short: " + value);
        } else {
            return Short.valueOf(value.shortValue());
        }
    }

}
