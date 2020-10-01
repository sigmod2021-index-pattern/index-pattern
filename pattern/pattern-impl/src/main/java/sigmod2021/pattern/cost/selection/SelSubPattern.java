package sigmod2021.pattern.cost.selection;

import sigmod2021.pattern.cost.transform.SubPattern;
import sigmod2021.pattern.cost.transform.SubPatternCondition;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 *
 */
public class SelSubPattern {


    private List<ICondition> conditions = new ArrayList<>();

    public SelSubPattern(SubPattern sp, Map<SubPatternCondition.ConditionId, PatternStats.ConditionStats> statsMap) {
        for (var c : sp.getConditions()) {
            var stats = statsMap.get(c.getId());
            conditions.add(new ICondition(c, stats.getGlobalSelectivity()));
        }
        Collections.sort(conditions);
    }

    public List<ICondition> getConditions() {
        return conditions;
    }

    public int getMaxSymbols() {
        return conditions.size();
    }

    public void setExecConfig(int n) {
        for (int i = 0; i < Math.min(n, conditions.size()); i++) {
            conditions.get(i).condition.enable();
        }
    }

    public static class ICondition implements Comparable<ICondition> {

        SubPatternCondition<?> condition;
        double selectivity;

        public ICondition(SubPatternCondition<?> condition, double selectivity) {
            this.condition = condition;
            this.selectivity = selectivity;
        }

        /**
         * @return the condition
         */
        public SubPatternCondition<?> getCondition() {
            return this.condition;
        }

        /**
         * @{inheritDoc}
         */
        @Override
        public int compareTo(ICondition o) {
            return Double.compare(selectivity, o.selectivity);
        }
    }

}
