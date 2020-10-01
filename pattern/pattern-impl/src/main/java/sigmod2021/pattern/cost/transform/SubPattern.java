package sigmod2021.pattern.cost.transform;

import java.util.List;

public class SubPattern {

    /** The index inside the full pattern */
    private final int index;

    /** States, if candidates are true matches given all conditions are evaluated */
    private final boolean exactPossible;

    /** Full length */
    private final int length;

    /** Conditions applicable for 2ndary indexing */
    private final List<SubPatternCondition<?>> conditions;

    /**
     * Creates a new SubPattern instance
     * @param length
     * @param conditions
     */
    public SubPattern(int index, int length, boolean exactPossible, List<SubPatternCondition<?>> conditions) {
        this.index = index;
        this.length = length;
        this.conditions = conditions;
        this.exactPossible = exactPossible;
    }


    /**
     * @return the index
     */
    public int getIndex() {
        return this.index;
    }

    /**
     * @return the length
     */
    public int getLength() {
        return this.length;
    }

    /**
     * @return the conditions
     */
    public List<SubPatternCondition<?>> getConditions() {
        return this.conditions;
    }


    /**
     * @return the exactPossible
     */
    public boolean isExactPossible() {
        return this.exactPossible;
    }

    /**
     * @{inheritDoc}
     */
    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((this.conditions == null) ? 0 : this.conditions.hashCode());
        result = prime * result + (this.exactPossible ? 1231 : 1237);
        result = prime * result + this.index;
        result = prime * result + this.length;
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
        SubPattern other = (SubPattern) obj;
        if (this.conditions == null) {
            if (other.conditions != null)
                return false;
        } else if (!this.conditions.equals(other.conditions))
            return false;
        if (this.exactPossible != other.exactPossible)
            return false;
        if (this.index != other.index)
            return false;
        if (this.length != other.length)
            return false;
        return true;
    }


    /**
     * @{inheritDoc}
     */
    @Override
    public String toString() {
        return String.format("SubPattern [index: %s, length: %s, exactPossible: %s, conditions: %s]", this.index,
                this.length, this.exactPossible, this.conditions);
    }

}
