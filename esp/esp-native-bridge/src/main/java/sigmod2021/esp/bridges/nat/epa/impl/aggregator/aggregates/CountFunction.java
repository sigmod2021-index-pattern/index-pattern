package sigmod2021.esp.bridges.nat.epa.impl.aggregator.aggregates;

/**
 * Partial aggregate for counting events.
 */
public class CountFunction implements AggregateFunction<Long, Long, Object> {

    private static final long serialVersionUID = 1L;

    /**
     * @{inheritDoc
     */
    @Override
    public Long fInit() {
        return 0L;
    }

    /**
     * @{inheritDoc
     */
    @Override
    public Long fInit(Object initialValue) {
        return 1L;
    }

    /**
     * @{inheritDoc
     */
    @Override
    public Long fMerge(Long left, Long right) {
        return left + right;
    }

    /**
     * @{inheritDoc
     */
    @Override
    public Long fEval(Long value, long counter) {
        return value;
    }
}
