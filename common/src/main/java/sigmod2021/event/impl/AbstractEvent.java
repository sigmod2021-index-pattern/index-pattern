package sigmod2021.event.impl;

import sigmod2021.event.Event;

import java.util.Objects;

/**
 * Basic implementation of the {@link Event} interface
 * providing general implementations of equals, hashCode and toString.
 */
public abstract class AbstractEvent implements Event {

    /**
     * {@inheritDoc}
     */
    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        for (int i = 0; i < getNumberOfAttributes(); i++) {
            result = prime * result + Objects.hashCode(get(i));
        }
        result = prime * result + (int) (getT1() ^ (getT1() >>> 32));
        result = prime * result + (int) (getT2() ^ (getT2() >>> 32));
        return result;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (!(obj instanceof Event))
            return false;
        Event other = (Event) obj;
        if (getT1() != other.getT1())
            return false;
        if (getT2() != other.getT2())
            return false;
        if (getNumberOfAttributes() != other.getNumberOfAttributes())
            return false;

        for (int i = 0; i < getNumberOfAttributes(); i++)
            if (!Objects.equals(get(i), other.get(i)))
                return false;
        return true;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String toString() {
        StringBuilder result = new StringBuilder("[");
        for (int i = 0; i < getNumberOfAttributes(); i++) {
            result.append(get(i));
            result.append(", ");
        }
        result.append(getT1()).append(", ").append(getT2()).append("]");
        return result.toString();
    }
}
