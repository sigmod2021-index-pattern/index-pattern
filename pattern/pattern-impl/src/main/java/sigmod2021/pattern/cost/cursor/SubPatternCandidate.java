package sigmod2021.pattern.cost.cursor;


import sigmod2021.db.event.TID;

/**
 *
 */
public class SubPatternCandidate {

    private final TID tid;
    private final long minTs;
    private final long maxTs;

    private final long minSeq;
    private final long maxSeq;

    /**
     * Creates a new SubPatternCandidate instance
     * @param minTs
     * @param maxTs
     * @param minSeq
     * @param maxSeq
     */
    public SubPatternCandidate(TID tid, long minTs, long maxTs, long minSeq, long maxSeq) {
        this.tid = tid;
        this.minTs = minTs;
        this.maxTs = maxTs;
        this.minSeq = minSeq;
        this.maxSeq = maxSeq;
    }


    /**
     * @return the minTs
     */
    public long getMinTs() {
        return this.minTs;
    }


    /**
     * @return the maxTs
     */
    public long getMaxTs() {
        return this.maxTs;
    }


    /**
     * @return the minSeq
     */
    public long getMinSeq() {
        return this.minSeq;
    }


    /**
     * @return the maxSeq
     */
    public long getMaxSeq() {
        return this.maxSeq;
    }

    public TID getTid() {
        return tid;
    }
}
