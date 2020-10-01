package sigmod2021.esp.api.util;

import sigmod2021.esp.api.epa.pattern.Partition;
import sigmod2021.event.Event;
import sigmod2021.event.EventSchema;
import sigmod2021.event.SchemaException;

import java.util.ArrayList;
import java.util.List;

/**
 * Used to partition events by a subset of their attributes
 */
public final class EventPartitioner {

    /**
     * Key used if no attributes were given
     */
    private static final Object EMPTY_KEY = new Object();

    /**
     * The separator used in building the key
     */
    private static final String SEPARATOR = ":";

    /**
     * The attribute indexes used for the key
     */
    private final int[] idxs;


    public EventPartitioner(EventSchema schema, Partition p) throws SchemaException {
        List<String> attribs = new ArrayList<>();
        p.forEach(attribs::add);


        idxs = new int[attribs.size()];
        for (int i = 0; i < idxs.length; i++)
            idxs[i] = schema.getAttributeIndex(attribs.get(i));
    }

    /**
     * Constructs a new EventPartitioner instance
     *
     * @param keyIdxs the indexes of the attributes to use for the key
     */
    public EventPartitioner(int... keyIdxs) {
        this.idxs = keyIdxs;
    }

    /**
     * Builds the key for the given event by using the defined attributes
     *
     * @param event the event to build the key for
     * @return the key for the given event
     */
    public Object buildKey(Event event) {
        if (idxs.length == 1)
            return event.get(idxs[0]);
        else if (idxs.length == 0)
            return EMPTY_KEY;
        else {
            StringBuilder res = new StringBuilder();
            for (int i : idxs) {
                res.append(event.get(i).toString()).append(SEPARATOR);
            }
            return res.toString();
        }
    }

    public boolean isEmpty() {
        return idxs.length == 0;
    }

}
