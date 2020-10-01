package sigmod2021.pattern.cost.estimation;

import sigmod2021.db.util.TimeInterval;

import java.util.List;

public class SubTreeDescription {

    /** Unique increasing id, assigned when scanning the tree at a specific level */
    final long id;

    /** The time interval covered by the sub-tree */
    final TimeInterval interval;

    /** The number of events represented by this sub-tree */
    final long eventCount;

    /** Statistics for each condition */
    final List<ConditionInfo> infos;

    /**
     * Creates a new SubTreeDescription instance
     * @param interval
     * @param infos
     */
    public SubTreeDescription(long id, TimeInterval interval, long eventCount, List<ConditionInfo> infos) {
        this.id = id;
        this.interval = interval;
        this.eventCount = eventCount;
        this.infos = infos;
    }

    /**
     * @return the id
     */
    public long getId() {
        return this.id;
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
     * @return the infos
     */
    public List<ConditionInfo> getInfos() {
        return this.infos;
    }

    /**
     * @{inheritDoc}
     */
    @Override
    public String toString() {
        return String.format("SubTreeDescription [id: %s, interval: %s, infos: %s, eventCount: %s]", this.id,
                this.interval, this.infos, this.eventCount);
    }
}
