package sigmod2021.pattern.cost.transform;

import sigmod2021.db.core.primaryindex.queries.range.AttributeRange;
import sigmod2021.db.util.TimeInterval;
import sigmod2021.esp.api.epa.PatternMatcher;
import sigmod2021.pattern.cost.selection.PatternStats;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class TransformedPattern {

    private final PatternMatcher def;

    ;
    private final boolean exactPossible;
    private final List<SubPattern> sps;
    private final ExtendMode lwMode;
    private final long lwHead;
    private final long lwTail;

    /**
     * Creates a new TransformedPattern instance
     * @param sps
     */
    public TransformedPattern(PatternMatcher src, List<SubPattern> sps) {
        this.def = src;
        this.sps = sps;
        // TODO: Handle non-determinism in PM
//		this.exactPossible = sps.stream().allMatch(x -> x.isExactPossible());
        this.exactPossible = false;

        var conditions = collectConditions();

        if (exactPossible) {
            lwMode = ExtendMode.EXACT;
            lwHead = 0;
            lwTail = 0;
        } else if (!sps.get(0).getConditions().isEmpty()) {
            lwMode = ExtendMode.START_EXACT;
            lwHead = sps.get(0).getConditions().get(0).getId().absolutePosition;
            lwTail = 0;
        } else if (conditions.stream().allMatch(x -> x.getId().subPatternIndex == sps.size() - 1)) {
            lwMode = ExtendMode.END_EXACT;
            var sp = sps.get(sps.size() - 1);
            lwHead = 0;
            lwTail = sp.getLength() - sp.getConditions().get(sp.getConditions().size() - 1).getId().absolutePosition - 1;
        } else {
            lwMode = ExtendMode.DEFAULT;
            var fst = conditions.get(0);
            var lst = conditions.get(conditions.size() - 1);
            long before = fst.getId().absolutePosition;
            long after = sps.get(lst.getId().subPatternIndex).getLength() - lst.getId().absolutePosition - 1;

            for (int spIndex = 0; spIndex < sps.size(); spIndex++) {
                var sp = sps.get(spIndex);
                if (spIndex < fst.getId().subPatternIndex)
                    before += sp.getLength();
                if (spIndex > lst.getId().subPatternIndex)
                    after += sp.getLength();
            }
            lwHead = before;
            lwTail = after;
        }
    }

    public PatternMatcher getDefinition() {
        return def;
    }

    public long getWindow() {
        return def.getWithin();
    }

    public void enableAll() {
        for (var sp : sps) {
            for (var c : sp.getConditions()) {
                c.enable();
            }
        }
    }

    public void disableAll() {
        for (var sp : sps) {
            for (var c : sp.getConditions()) {
                c.disable();
            }
        }
    }

    public List<SubPatternCondition<?>> collectConditions() {
        List<SubPatternCondition<?>> result = new ArrayList<>();
        for (SubPattern sp : sps)
            for (SubPatternCondition<?> spc : sp.getConditions())
                result.add(spc);
        return result;
    }

    public Iterator<ExecutableConfiguration> iterateConfigurations() {
        return new Iterator<ExecutableConfiguration>() {

            final List<SubPatternCondition<?>> conditions = collectConditions();

            final int max = (1 << conditions.size()) - 1;

            int next = 1;

            @Override
            public boolean hasNext() {
                return next <= max;
            }

            @Override
            public ExecutableConfiguration next() {
                disableAll();
                for (int i = 0; i < conditions.size(); i++) {
                    if ((next & (1 << i)) != 0)
                        conditions.get(i).enable();
                }
                next++;
                return createExecution();
            }
        };
    }

    /**
     * @return the sps
     */
    public List<SubPattern> getSubPatterns() {
        return this.sps;
    }

    public ExecutableConfiguration createExecution() {
        boolean exact = exactPossible;
        List<ExecutableSubPattern> result = new ArrayList<>();
        long minDist = 0;


        for (int i = 0; i < sps.size(); i++) {
            var sp = sps.get(i);
            List<ExecutableCondition> conditions = new ArrayList<>();

            long offset = 0;
            boolean fst = true;
            for (var c : sp.getConditions()) {
                if (!c.isEnabled()) {
                    exact = false;
                    continue;
                }

                if (fst) {
                    conditions.add(new ExecutableCondition(c, minDist + c.getId().absolutePosition, Long.MAX_VALUE));
                } else {
                    conditions.add(new ExecutableCondition(c, c.getId().absolutePosition - offset, c.getId().absolutePosition - offset));
                }
                offset = c.getId().absolutePosition;
                fst = false;
            }
            if (!conditions.isEmpty()) {
                result.add(new ExecutableSubPattern(sp, conditions));
            }

            minDist = (fst) ? minDist + sp.getLength() : (sp.getLength() - offset);
        }
        return new ExecutableConfiguration(result, exact);
    }

    /**
     * @{inheritDoc}
     */
    @Override
    public String toString() {
        return String.format("TransformedPattern [sps: %s]", this.sps);
    }

    public TimeInterval getLightweightTScope(PatternStats.MatchInterval candidate) {
        // Assume lightweight indexing on every attribute
        switch (lwMode) {
            case EXACT: {
                return candidate.getInterval();
            }
            case START_EXACT: {
                TimeInterval ti = candidate.getInterval();
                long end = Math.max(candidate.getExtendEnd() - lwHead + def.getWithin(), ti.getT2());
                return new TimeInterval(ti.getT1(), end);
            }
            case END_EXACT: {
                TimeInterval ti = candidate.getInterval();
                long begin = Math.min(candidate.getExtendBegin() + lwTail - def.getWithin(), ti.getT1());
                return new TimeInterval(begin, ti.getT2());
            }
            default: {
                TimeInterval ti = candidate.getInterval();
                long begin = Math.min(candidate.getExtendBegin() + lwTail - def.getWithin(), ti.getT1());
                long end = Math.max(candidate.getExtendEnd() - lwHead + def.getWithin(), ti.getT2());
                return new TimeInterval(begin, end);
            }
        }
    }

    public static enum ExtendMode {EXACT, START_EXACT, END_EXACT, DEFAULT}

    /* ********************************************************************************
     *
     * INNER CLASSES
     *
     * ********************************************************************************/

    public static class ExecutableSubPattern {
        private SubPattern source;
        private List<ExecutableCondition> conditions;

        /**
         * Creates a new ExecutableSubPattern instance
         * @param source
         * @param conditions
         */
        public ExecutableSubPattern(SubPattern source, List<ExecutableCondition> conditions) {
            this.source = source;
            this.conditions = conditions;
        }

        /**
         * @return the index
         */
        public int getIndex() {
            return source.getIndex();
        }

        /**
         * @return the length
         */
        public int getLength() {
            return source.getLength();
        }

        /**
         * @return the conditions
         */
        public List<ExecutableCondition> getConditions() {
            return this.conditions;
        }


        /**
         * @{inheritDoc}
         */
        @Override
        public String toString() {
            return String.format("ExecutableSubPattern [index: %d, length: %d, conditions: %s]",
                    this.getIndex(), this.getLength(), this.conditions);
        }

        /**
         * @{inheritDoc}
         */
        @Override
        public int hashCode() {
            final int prime = 31;
            int result = 1;
            result = prime * result + ((this.conditions == null) ? 0 : this.conditions.hashCode());
            result = prime * result + ((this.source == null) ? 0 : this.source.hashCode());
            return result;
        }

        /**
         * @{inheritDoc}
         */
        @Override
        public boolean equals(Object obj) {
            if (this == obj)
                return true;
            if (obj == null)
                return false;
            if (getClass() != obj.getClass())
                return false;
            ExecutableSubPattern other = (ExecutableSubPattern) obj;
            if (this.conditions == null) {
                if (other.conditions != null)
                    return false;
            } else if (!this.conditions.equals(other.conditions))
                return false;
            if (this.source == null) {
                if (other.source != null)
                    return false;
            } else if (!this.source.equals(other.source))
                return false;
            return true;
        }
    }

    public static class ExecutableCondition {

        private final SubPatternCondition<?> source;
        private final long minDist;
        private final long maxDist;

        /**
         * Creates a new ExecutableCondition instance
         * @param range
         * @param minDist
         * @param maxDist
         */
        public ExecutableCondition(SubPatternCondition<?> source, long minDist,
                                   long maxDist) {
            this.source = source;
            this.minDist = minDist;
            this.maxDist = maxDist;
        }

        /**
         * @{inheritDoc}
         */
        @Override
        public int hashCode() {
            final int prime = 31;
            int result = 1;
            result = prime * result + (int) (this.maxDist ^ (this.maxDist >>> 32));
            result = prime * result + (int) (this.minDist ^ (this.minDist >>> 32));
            result = prime * result + ((this.source == null) ? 0 : this.source.hashCode());
            return result;
        }


        /**
         * @{inheritDoc}
         */
        @Override
        public boolean equals(Object obj) {
            if (this == obj)
                return true;
            if (obj == null)
                return false;
            if (getClass() != obj.getClass())
                return false;
            ExecutableCondition other = (ExecutableCondition) obj;
            if (this.maxDist != other.maxDist)
                return false;
            if (this.minDist != other.minDist)
                return false;
            if (this.source == null) {
                if (other.source != null)
                    return false;
            } else if (!this.source.equals(other.source))
                return false;
            return true;
        }


        /**
         * @return the range
         */
        public AttributeRange<?> getRange() {
            return source.getRange();
        }

        public SubPatternCondition.ConditionId getId() {
            return source.getId();
        }

        public char getSymbol() {
            return source.getSymbol();
        }

        /**
         * @return the minDist
         */
        public long getMinDist() {
            return this.minDist;
        }

        /**
         * @return the maxDist
         */
        public long getMaxDist() {
            return this.maxDist;
        }

        /**
         * @{inheritDoc}
         */
        @Override
        public String toString() {
            return String.format(
                    "ExecutableCondition [subPatternIndex: %s, absolutePosition: %s, range: %s, minDist: %s, maxDist: %s]",
                    this.getId().subPatternIndex, this.getId().absolutePosition, this.getRange(), this.minDist, this.maxDist);
        }
    }

    public class ExecutableConfiguration {
        private final List<ExecutableSubPattern> subPatterns;
        private final ExtendMode mode;

        private final int minCandidateLength;

        private ExecutableConfiguration(List<ExecutableSubPattern> subPatterns, boolean exact) {
            this.subPatterns = subPatterns;

            // Exact
            if (exact) {
                this.mode = ExtendMode.EXACT;
            }
            // Extend towards begin of pattern
            else if (!subPatterns.isEmpty() && subPatterns.get(0).getIndex() == sps.size() - 1) {
                this.mode = ExtendMode.END_EXACT;
            }
            // Extend at the beginning
            else if (!subPatterns.isEmpty() && subPatterns.get(0).getIndex() == 0) {
                this.mode = ExtendMode.START_EXACT;
            }
            // Default extension
            else {
                this.mode = ExtendMode.DEFAULT;
            }
            this.minCandidateLength = subPatterns.stream().map(x -> x.getLength()).reduce(0, (x, y) -> x + y);
        }

        /**
         * @{inheritDoc}
         */
        @Override
        public int hashCode() {
            final int prime = 31;
            int result = 1;
            result = prime * result + getOuterType().hashCode();
            result = prime * result + ((this.subPatterns == null) ? 0 : this.subPatterns.hashCode());
            return result;
        }

        /**
         * @{inheritDoc}
         */
        @Override
        public boolean equals(Object obj) {
            if (this == obj)
                return true;
            if (obj == null)
                return false;
            if (getClass() != obj.getClass())
                return false;
            ExecutableConfiguration other = (ExecutableConfiguration) obj;
            if (!getOuterType().equals(other.getOuterType()))
                return false;
            if (this.subPatterns == null) {
                if (other.subPatterns != null)
                    return false;
            } else if (!this.subPatterns.equals(other.subPatterns))
                return false;
            return true;
        }


        public boolean isExact() {
            return mode == ExtendMode.EXACT;
        }

        /**
         * @return the minCandidateLength
         */
        public int getMinCandidateLength() {
            return this.minCandidateLength;
        }

        public List<ExecutableSubPattern> getSubPatterns() {
            return subPatterns;
        }

        public List<ExecutableCondition> getConditions() {
            List<ExecutableCondition> result = new ArrayList<>();
            for (var sp : subPatterns) {
                for (var c : sp.conditions)
                    result.add(c);
            }
            return result;
        }

//		public double getTScopeDuration(double candidateLength) {
//			switch ( mode ) {
//				case EXACT:
//					return candidateLength;
//				case START_EXACT:
//				case END_EXACT:
//					return def.getWithin();
//				default:
//					return (2*def.getWithin()) - (candidateLength+head+tail);
//			}
//		}

        public <T extends TimeInterval> T getTScope(T candidate, long window) {
            switch (mode) {
                case EXACT:
                    return candidate;
                case START_EXACT: {
                    return (T) candidate.adjust(candidate.getT1(), candidate.getT1() + window);
                }
                case END_EXACT: {
                    return (T) candidate.adjust(candidate.getT2() - window, candidate.getT2());
                }
                default: {
                    return (T) candidate.adjust(candidate.getT2() - window, candidate.getT1() + window);
                }
            }
        }

        /**
         * @{inheritDoc}
         */
        @Override
        public String toString() {
            return String.format("ExecutableConfiguration [mode: %s, conditions: %s]", this.mode, this.subPatterns);
        }


        private TransformedPattern getOuterType() {
            return TransformedPattern.this;
        }
    }
}
