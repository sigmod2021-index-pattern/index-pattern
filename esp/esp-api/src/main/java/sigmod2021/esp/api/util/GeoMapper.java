package sigmod2021.esp.api.util;

import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.GeometryFactory;
import com.vividsolutions.jts.io.ParseException;
import com.vividsolutions.jts.io.WKTReader;
import sigmod2021.event.Attribute;
import sigmod2021.event.Attribute.DataType;
import sigmod2021.event.Event;
import sigmod2021.event.EventSchema;
import sigmod2021.event.impl.SimpleEvent;

import java.util.ArrayList;
import java.util.List;

/**
 * Two way mapper for geometric attributes:<br>
 * WKT <=> Native
 */
public class GeoMapper {

    /**
     * The reader for parsing WKT
     */
    private final WKTReader reader;

    /**
     * The length of input events
     */
    private final int inputLength;

    /**
     * The indices of geometry attributes
     */
    private final int[] geoIndices;

    /**
     * Constructs a new GeoMapper-instance
     *
     * @param inputSchema the schema of incoming events
     * @param schemacount the length of the schema of incoming events
     */
    public GeoMapper(EventSchema schema) {
        this(schema, new GeometryFactory());
    }

    /**
     * Constructs a new GeoMapper-instance
     *
     * @param inputSchema the schema of incoming events
     * @param schemacount the length of the schema of incoming events
     * @param factory     the geometry-factory to use
     */
    public GeoMapper(EventSchema schema, GeometryFactory factory) {
        this.inputLength = schema.getNumAttributes();
        this.reader = new WKTReader(factory);

        // Collect geometry indices
        List<Integer> indices = new ArrayList<Integer>();
        int k = 0;
        for (Attribute a : schema) {
            if (a.getType() == DataType.GEOMETRY)
                indices.add(k);
            k++;
        }

        if (!indices.isEmpty()) {
            geoIndices = new int[indices.size()];
            for (int i = 0; i < geoIndices.length; i++) {
                geoIndices[i] = indices.get(i);
            }
        } else {
            geoIndices = null;
        }
    }

    /**
     * Maps incoming events (WKT => Native)
     *
     * @param event the event to process
     * @return the result. Is the same object if no transformation was necessary.
     */
    public Object[] mapIncoming(Object[] event) {
        if (geoIndices == null)
            return event;
        else {
            Object[] res = new Object[inputLength];
            System.arraycopy(event, 0, res, 0, inputLength);
            for (int i : geoIndices) {
                if (res[i] instanceof Geometry)
                    continue;
                try {
                    res[i] = reader.read(res[i].toString());
                } catch (ParseException e) {
                    throw new IllegalArgumentException("Illegal WKT-String", e);
                }
            }
            return res;
        }
    }

    /**
     * Maps outgoing events (Native => WKT)
     *
     * @param event the event to process
     * @return the result. Is the same object if no transformation was necessary.
     */
    public Event mapOutgoing(Event event) {
        if (geoIndices == null)
            return event;
        else {
            int payloadlen = event.getNumberOfAttributes();
            Object[] payload = new Object[payloadlen];
            for (int i = 0; i < payloadlen; i++)
                payload[i] = event.get(i);

            for (int i : geoIndices) {
                if (payload[i] instanceof Geometry) {
                    payload[i] = ((Geometry) payload[i]).toText();
                }
            }
            return new SimpleEvent(payload, event.getT1(), event.getT2());
        }
    }

}
