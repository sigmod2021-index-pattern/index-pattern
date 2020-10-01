package sigmod2021.esp.api.epa.pattern.regex;


public class Atom extends Pattern {

    private final char symbol;

    public Atom(char symbol) {
        this.symbol = symbol;
    }

    public char getSymbol() {
        return symbol;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + symbol;
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        Atom other = (Atom) obj;
        if (symbol != other.symbol)
            return false;
        return true;
    }

    @Override
    public String toString() {
        return "" + getSymbol();
    }


}
