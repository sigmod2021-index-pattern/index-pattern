package sigmod2021.pattern.cost.selection.impl;

import sigmod2021.db.core.secondaryindex.SecondaryTimeIndex;
import sigmod2021.db.core.primaryindex.impl.PrimaryIndexImpl;
import sigmod2021.pattern.cost.execution.ExecutionStrategy;
import sigmod2021.pattern.cost.selection.CostEstimator;
import sigmod2021.pattern.cost.selection.IndexSelectionStrategy;
import sigmod2021.pattern.cost.selection.PatternStats;
import sigmod2021.pattern.cost.transform.TransformedPattern;

import java.util.List;

/**
 *
 */
public class HistogramOnlyStrategySelector implements IndexSelectionStrategy {


    private final CostEstimator estimator;

    public HistogramOnlyStrategySelector(PrimaryIndexImpl primary, List<? extends SecondaryTimeIndex<?>> secondaries,
                                         TransformedPattern pattern, PatternStats result) {
        this.estimator = new CostEstimator(primary, secondaries, pattern, result);
    }

    /**
     * @{inheritDoc}
     */
    @Override
    public String getName() {
        return "Lightweight";
    }

    /**
     * @{inheritDoc}
     */
    @Override
    public Result selectIndexes() {
        long time = -System.currentTimeMillis();
        ExecutionStrategy strategy = estimator.createLightweightStrategy();
        time += System.currentTimeMillis();
        return new Result(strategy, 1, time);
    }
}
