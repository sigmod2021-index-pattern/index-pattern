package sigmod2021.esp.api.expression.arithmetic.atomic;

import sigmod2021.event.Attribute.DataType;

/**
 * A constant byte value
 */
public class ByteConstant extends Constant<Byte> {

    /**
     * @param value The value
     */
    public ByteConstant(Byte value) {
        super(value, DataType.BYTE);
    }

    public ByteConstant(Integer value) {
        this(convert(value));
    }

    private static Byte convert(Integer value) {
        if (value > Byte.MAX_VALUE || value < Byte.MIN_VALUE) {
            throw new IllegalArgumentException("Value out of range for byte: " + value);
        } else {
            return Byte.valueOf(value.byteValue());
        }
    }

}
