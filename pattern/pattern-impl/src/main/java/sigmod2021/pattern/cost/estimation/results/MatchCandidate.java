package sigmod2021.pattern.cost.estimation.results;

import sigmod2021.pattern.cost.estimation.SubTreeDescription;
import sigmod2021.db.util.TimeInterval;
import sigmod2021.pattern.cost.selection.PatternStats;

import java.util.ArrayList;
import java.util.List;

public class MatchCandidate implements PatternStats.MatchInterval {
    private final List<ConditionCandidates> infos;

    private TimeInterval interval;

    private long eventCount;

    private long lastId;

    private long extendEnd = -1L;

    private long extendBegin = -1L;

    MatchCandidate(SubTreeDescription std) {
        this.interval = std.getInterval();
        this.eventCount = std.getEventCount();
        this.lastId = std.getId();

        this.infos = new ArrayList<>();
        for (int i = 0; i < std.getInfos().size(); i++) {
            var conditionCandidates = new ConditionCandidates();
            conditionCandidates.update(std, i);
            infos.add(conditionCandidates);
        }

        // Set t1 for trim
        if (!std.getInfos().get(0).isHit())
            throw new IllegalArgumentException("First sub-tree of a match candidate must contain a hit for first condition.");

        extendEnd = std.getInterval().getT2();

    }

    /**
     * @return the extendEnd
     */
    public long getExtendEnd() {
        return this.extendEnd;
    }

    /**
     * @return the extendBegin
     */
    public long getExtendBegin() {
        return this.extendBegin;
    }


    void update(SubTreeDescription std) {
        if (std.getId() - this.lastId == 1)
            this.interval = new TimeInterval(this.interval.getT1(), std.getInterval().getT2());
        else
            throw new IllegalArgumentException("Can only update with consecutive sub-trees");

        this.eventCount += std.getEventCount();
        for (int i = 0; i < std.getInfos().size(); i++) {
            infos.get(i).update(std, i);
        }
        lastId = std.getId();

        if (std.getInfos().get(0).isHit())
            extendEnd = std.getInterval().getT2();

        // Set t2 for trim
        if (std.getInfos().get(std.getInfos().size() - 1).isHit() && extendBegin < 0)
            extendBegin = std.getInterval().getT1();
    }

    // TODO: Questionable
//	void trim(long window) {
//		
//		long maxTS = Math.max( interval.getT1(), extendBegin - window );
//		long minTE = Math.min( interval.getT2(), extendEnd + window );
//		
//		interval = new TimeInterval(maxTS, minTE);
//		for ( var i : infos )  {
//			while ( i.matches.get(0).interval.isBelow(interval) )
//				i.matches.remove(0);
//			
//			var m = i.matches.get(0);
//			m.interval = new TimeInterval(Math.max(m.interval.getT1(), maxTS), m.interval.getT2());
//			
//			while ( i.matches.get(i.matches.size()-1).interval.isAbove(interval) )
//				i.matches.remove(i.matches.size()-1);
//			
//			m = i.matches.get(i.matches.size()-1);
//			m.interval = new TimeInterval(m.interval.getT1(), Math.min(m.interval.getT2(),minTE));
//		}
//	}

    /**
     * @return the infos
     */
    public List<ConditionCandidates> getInfos() {
        return this.infos;
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
        StringBuilder result = new StringBuilder();
        result.append("MatchCandidate [").append(String.format("%n"));
        result.append("  interval: ").append(interval).append(String.format("%n"));
        result.append("  eventCount: ").append(eventCount).append(String.format("%n"));
        for (int i = 0; i < infos.size(); i++) {
            result.append("  ").append(i).append(": ").append(infos.get(i)).append(String.format("%n"));
        }
        result.append("]");
        return result.toString();
    }


}
