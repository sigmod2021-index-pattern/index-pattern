package sigmod2021.db.core.primaryindex.impl.legacy;

import xxl.core.functions.AbstractFunction;
import xxl.core.functions.Function;
import xxl.core.indexStructures.BPlusTree.KeyRange;

@SuppressWarnings("deprecation")
public class LongKeyRange extends KeyRange {

    public static final Function<Long, LongKeyRange> FACTORY_FUNCTION = new AbstractFunction<Long, LongKeyRange>() {

        private static final long serialVersionUID = 1L;

        @Override
        public LongKeyRange invoke(final Long min, final Long max) {
            return new LongKeyRange(min, max);
        }
    };

    public LongKeyRange(final Long min, final Long max) {
        super(min, max);
    }

    public LongKeyRange(final long min, final long max) {
        this(new Long(min), new Long(max));
    }

    /**
     * @{inheritDoc}
     */
    @Override
    public Long minBound() {
        return (Long) super.minBound();
    }

    /**
     * @{inheritDoc}
     */
    @Override
    public Long maxBound() {
        return (Long) super.maxBound();
    }

    @Override
    public Object clone() {
        return new LongKeyRange(((Long) this.sepValue).longValue(), ((Long) this.maxBound).longValue());
    }

}
