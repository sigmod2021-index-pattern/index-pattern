package sigmod2021.pattern.cost.selection;

import sigmod2021.pattern.cost.transform.SubPatternCondition;
import sigmod2021.db.util.TimeInterval;

import java.util.Iterator;
import java.util.List;

/**
 *
 */
public interface PatternStats {

    List<MatchInterval> getMatchIntervals();

    List<ConditionStats> getConditionStats();

    interface MatchInterval {
        TimeInterval getInterval();

        long getEventCount();

        long getExtendEnd();

        long getExtendBegin();
    }

    interface ConditionStats {
        SubPatternCondition.ConditionId getConditionId();

        long getTotalEventCount();

        long getTotalDuration();

        int getNumIntervals();

        double getSelectedEventCount();

        Iterator<TimeInterval> intervals();

        double getGlobalSelectivity();

        double getLocalSelectivity();
    }

}
