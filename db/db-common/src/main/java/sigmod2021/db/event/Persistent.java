package sigmod2021.db.event;

/**
 *
 */
public class Persistent<T> {

    protected final TID id;

    protected final T item;

    /**
     * Creates a new Persistent instance
     */
    public Persistent(final TID id, final T item) {
        this.id = id;
        this.item = item;
    }

    /**
     * @return the physical id of the contained item
     */
    public TID getId() {
        return id;
    }

    public T getItem() {
        return item;
    }

    /**
     * @{inheritDoc}
     */
    @Override
    public String toString() {
        return String.format("%s -> %s", this.id, this.item);
    }

    /**
     * @{inheritDoc}
     */
    @Override
    public int hashCode() {
        final int prime = 31;
        int result = ((this.id == null) ? 0 : this.id.hashCode());
        result = prime * result + ((this.item == null) ? 0 : item.hashCode());
        return result;
    }

    /**
     * @{inheritDoc}
     */
    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (!(obj instanceof Persistent))
            return false;
        Persistent<?> other = (Persistent<?>) obj;
        if (this.id == null) {
            if (other.getId() != null)
                return false;
        } else if (!this.id.equals(other.getId()))
            return false;

        if (this.item == null) {
            if (other.item != null)
                return false;
        } else if (!this.item.equals(other.item))
            return false;
        return true;
    }

}
