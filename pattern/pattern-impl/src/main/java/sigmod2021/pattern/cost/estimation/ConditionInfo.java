package sigmod2021.pattern.cost.estimation;

import sigmod2021.pattern.cost.transform.TransformedPattern.ExecutableCondition;

/**
 *
 */
public class ConditionInfo {

    private final ExecutableCondition condition;
    private final double selectivity;

    /**
     * Creates a new ConditionInfo instance
     * @param condition
     * @param hit
     */
    public ConditionInfo(ExecutableCondition condition, double selectivity) {
        this.condition = condition;
        this.selectivity = selectivity;
    }

    public boolean isHit() {
        return selectivity > 0;
    }


    /**
     * @return the condition
     */
    public ExecutableCondition getCondition() {
        return this.condition;
    }


    /**
     * @return the selectivity
     */
    public double getSelectivity() {
        return this.selectivity;
    }


}
