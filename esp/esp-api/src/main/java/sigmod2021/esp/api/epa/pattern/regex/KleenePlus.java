package sigmod2021.esp.api.epa.pattern.regex;


public class KleenePlus extends Pattern {

    private final Pattern input;

    public KleenePlus(Pattern input) {
        this.input = input;
    }

    /**
     * @return the input
     */
    public Pattern getInput() {
        return this.input;
    }


    @Override
    public String toString() {
        return "(" + input + ")+";
    }

    /**
     * @{inheritDoc}
     */
    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((this.input == null) ? 0 : this.input.hashCode());
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
        KleenePlus other = (KleenePlus) obj;
        if (this.input == null) {
            if (other.input != null)
                return false;
        } else if (!this.input.equals(other.input))
            return false;
        return true;
    }
}
