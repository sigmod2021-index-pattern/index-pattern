package sigmod2021.db.core.primaryindex.impl.legacy;

import xxl.core.functions.AbstractFunction;
import xxl.core.functions.Function;
import xxl.core.indexStructures.Separator;

@SuppressWarnings("deprecation")
public class LongSeparator extends Separator {

    public static final Function<Long, LongSeparator> FACTORY_FUNCTION = new AbstractFunction<Long, LongSeparator>() {

        private static final long serialVersionUID = 1L;

        @Override
        public LongSeparator invoke(final Long value) {
            return new LongSeparator(value);
        }
    };

    public LongSeparator(final Long value) {
        super(value);
    }

    public LongSeparator(final long value) {
        this(new Long(value));
    }

    @Override
    public Object clone() {
        return new LongSeparator(((Long) this.sepValue).longValue());
    }

}
