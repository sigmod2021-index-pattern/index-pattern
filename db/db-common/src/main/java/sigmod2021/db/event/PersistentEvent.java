package sigmod2021.db.event;

import sigmod2021.event.Event;


/**
 *
 */
public class PersistentEvent extends Persistent<Event> implements Event {

    /**
     * Creates a new PersistentEvent instance
     * @param id
     * @param item
     */
    public PersistentEvent(TID id, Event item) {
        super(id, item);
    }

    /**
     * @{inheritDoc}
     */
    @Override
    public Object get(int index) {
        return item.get(index);
    }

    /**
     * @{inheritDoc}
     */
    @Override
    public <T> T get(int index, Class<T> clazz) {
        return item.get(index, clazz);
    }

    /**
     * @{inheritDoc}
     */
    @Override
    public long getT1() {
        return item.getT1();
    }

    /**
     * @{inheritDoc}
     */
    @Override
    public long getT2() {
        return item.getT2();
    }

    /**
     * @{inheritDoc}
     */
    @Override
    public int getNumberOfAttributes() {
        return item.getNumberOfAttributes();
    }

    /**
     * @{inheritDoc}
     */
    @Override
    public PersistentEvent window(long t1, long t2) {
        return new PersistentEvent(id, item.window(t1, t2));
    }


    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder("{");
        for (int i = 0; i < getNumberOfAttributes(); i++) {
            builder.append(get(i)).append(", ");
        }
        builder.append(getT1());
        builder.append("}");
        return builder.toString();
    }
}
