package sigmod2021.pattern.cost.estimation.results;

import sigmod2021.pattern.cost.estimation.SubTreeDescription;
import sigmod2021.db.util.TimeInterval;

/**
 * Describes a contiguous candidate interval for a sub-pattern 
 *
 */
public class ConditionCandidate {
    long lastId;
    TimeInterval interval;
    long eventCount;
    double selectedEventCount;
    int subTreeCount;

    ConditionCandidate(SubTreeDescription std, int conditionIndex) {
        this.lastId = std.getId();
        this.interval = std.getInterval();
        this.selectedEventCount = std.getInfos().get(conditionIndex).getSelectivity() * std.getEventCount();
        this.eventCount = std.getEventCount();
        this.subTreeCount = 1;
    }

    void update(SubTreeDescription std, int conditionIndex) {
        if (std.getId() - lastId == 1)
            this.interval = new TimeInterval(this.interval.getT1(), std.getInterval().getT2());
        else
            throw new IllegalArgumentException("Can only update with consecutive intervals");

        this.selectedEventCount += std.getInfos().get(conditionIndex).getSelectivity() * std.getEventCount();
        this.subTreeCount++;
        this.eventCount += std.getEventCount();
        this.lastId = std.getId();
    }

    public double getLocalSelectivity() {
        return selectedEventCount / eventCount;
    }

    /**
     * @return the interval
     */
    public TimeInterval getInterval() {
        return this.interval;
    }

    /**
     * @return the eventCount
     */
    public long getEventCount() {
        return this.eventCount;
    }

    /**
     * @{inheritDoc}
     */
    @Override
    public String toString() {
        return String.format("ConditionCandidate [eventCount: %d, interval: %s, localselectivity: %.4f]",
                this.eventCount, this.interval, getLocalSelectivity());
    }
}
