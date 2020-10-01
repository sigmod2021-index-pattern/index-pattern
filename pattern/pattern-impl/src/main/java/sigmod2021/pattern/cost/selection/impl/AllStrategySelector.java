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
public class AllStrategySelector implements IndexSelectionStrategy {

    private final TransformedPattern pattern;

    private final CostEstimator estimator;

    public AllStrategySelector(PrimaryIndexImpl primary, List<? extends SecondaryTimeIndex<?>> secondaries,
                               TransformedPattern pattern, PatternStats result) {
        this.pattern = pattern;
        this.estimator = new CostEstimator(primary, secondaries, pattern, result);
    }

    /**
     * @{inheritDoc}
     */
    @Override
    public String getName() {
        return "All";
    }

    /**
     * @{inheritDoc}
     */
    @Override
    public Result selectIndexes() {
        long time = -System.currentTimeMillis();
        pattern.enableAll();
        pattern.createExecution();
        ExecutionStrategy strategy = estimator.createSecondaryStrategy(pattern.createExecution());
        time += System.currentTimeMillis();
        return new Result(strategy, 1, time);
    }


}
