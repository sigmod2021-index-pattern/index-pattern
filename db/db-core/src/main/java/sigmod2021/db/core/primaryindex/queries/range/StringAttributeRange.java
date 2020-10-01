package sigmod2021.db.core.primaryindex.queries.range;

import sigmod2021.event.Attribute.DataType;

/**
 */
public class StringAttributeRange extends AttributeRange<String> {

    /**
     * Creates a new StringAttributeRange instance
     *
     * @param name
     *            the attribute name
     * @param lower
     *            the lower bound (inclusive)
     * @param upper
     *            the upper bound (inclusive)
     */
    public StringAttributeRange(final String name, final String lower, final String upper, boolean lowerInclusive, boolean upperInclusive) {
        super(name, DataType.STRING, String.class, lower, upper, lowerInclusive, upperInclusive);
    }

}
