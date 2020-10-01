package sigmod2021.esp.api.epa.aggregates;

import java.io.Serializable;

public abstract class Aggregate implements Serializable {

    /**
     *
     */
    private static final long serialVersionUID = 1L;

    private final String attributeIn;

    private final String attributeOut;

    public Aggregate(String attributeIn, String attributeOut) {
        super();
        this.attributeIn = attributeIn;
        this.attributeOut = attributeOut;
    }

    public String getAttributeIn() {
        return attributeIn;
    }

    public String getAttributeOut() {
        return attributeOut;
    }


    @Override
    public String toString() {
        return getClass().getSimpleName() + "(" + attributeIn + ") AS " + attributeOut;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((attributeIn == null) ? 0 : attributeIn.hashCode());
        result = prime * result + ((attributeOut == null) ? 0 : attributeOut.hashCode());
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
        Aggregate other = (Aggregate) obj;
        if (attributeIn == null) {
            if (other.attributeIn != null)
                return false;
        } else if (!attributeIn.equals(other.attributeIn))
            return false;
        if (attributeOut == null) {
            if (other.attributeOut != null)
                return false;
        } else if (!attributeOut.equals(other.attributeOut))
            return false;
        return true;
    }
}
