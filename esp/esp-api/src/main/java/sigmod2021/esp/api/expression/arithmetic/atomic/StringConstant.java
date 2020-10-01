package sigmod2021.esp.api.expression.arithmetic.atomic;

import sigmod2021.event.Attribute.DataType;

/**
 * A constant string value
 */
public class StringConstant extends Constant<String> {

    /**
     * @param value The value
     */
    public StringConstant(String value) {
        super(value, DataType.STRING);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String toString() {
        return value == null ? "null" : "'" + value + "'";
    }
}
