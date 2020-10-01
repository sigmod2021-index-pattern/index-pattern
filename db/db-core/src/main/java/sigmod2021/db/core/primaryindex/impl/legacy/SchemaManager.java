package sigmod2021.db.core.primaryindex.impl.legacy;

import sigmod2021.event.Attribute;
import sigmod2021.event.Event;
import sigmod2021.event.EventSchema;
import sigmod2021.event.TimeRepresentation;
import sigmod2021.event.impl.SimpleEvent;
import xxl.core.io.converters.*;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * This Component manages all schemas of an event stream. It allows schema
 * changes and resolves the schema for queries. Every schema has an time
 * interval assigned within it is valid. Each schema change results in a new
 * entry.
 * <p>
 * The SchemaManager offers functionality to modify a streams schema
 */
public class SchemaManager {

    /**
     * Returns a data converter for the given schema.
     *
     * @param schema the schema of the events
     * @return a converter serializing the given event format
     */
    public static MeasuredConverter<Event> getDataConverter(final EventSchema schema, final TimeRepresentation rep) {
        return new MeasuredConverter<Event>() {

            private static final long serialVersionUID = 1L;

            final int eventSize = calculateEventSize(schema, rep);

            @Override
            public int getMaxObjectSize() {
                return this.eventSize;
            }

            @Override
            public Event read(final DataInput dataInput, final Event objects) throws IOException {
                Object[] payload = new Object[schema.getNumAttributes()];


                for (int i = 0; i < payload.length; ) {
                    switch (schema.getAttribute(i).getType()) {
                        case BYTE:
                            payload[i] = ByteConverter.DEFAULT_INSTANCE.readByte(dataInput);
                            break;
                        case SHORT:
                            payload[i] = ShortConverter.DEFAULT_INSTANCE.readShort(dataInput);
                            break;
                        case INTEGER:
                            payload[i] = IntegerConverter.DEFAULT_INSTANCE.readInt(dataInput);
                            break;
                        case LONG:
                            payload[i] = LongConverter.DEFAULT_INSTANCE.readLong(dataInput);
                            break;
                        case FLOAT:
                            payload[i] = FloatConverter.DEFAULT_INSTANCE.readFloat(dataInput);
                            break;
                        case DOUBLE:
                            payload[i] = DoubleConverter.DEFAULT_INSTANCE.readDouble(dataInput);
                            break;
                        case STRING:
                            payload[i] = StringConverter.DEFAULT_INSTANCE.read(dataInput);
                            break;
                        case GEOMETRY:
                            payload[i] = StringConverter.DEFAULT_INSTANCE.read(dataInput);
                            break;
                        default:
                            throw new IOException("Unknown data type:" + schema.getAttribute(i).getType());
                    }
                    i++;
                }

                long ts = LongConverter.DEFAULT_INSTANCE.readLong(dataInput);
                return rep == TimeRepresentation.INTERVAL ?
                        new SimpleEvent(payload, ts, LongConverter.DEFAULT_INSTANCE.readLong(dataInput)) :
                        new SimpleEvent(payload, ts, ts + 1);
            }

            @Override
            public void write(final DataOutput dataOutput, final Event objects) throws IOException {
                for (int i = 0; i < schema.getNumAttributes(); i++) {
                    switch (schema.getAttribute(i).getType()) {
                        case BYTE:
                            ByteConverter.DEFAULT_INSTANCE.writeByte(dataOutput, (Byte) objects.get(i));
                            break;
                        case SHORT:
                            ShortConverter.DEFAULT_INSTANCE.writeShort(dataOutput, (Short) objects.get(i));
                            break;
                        case INTEGER:
                            IntegerConverter.DEFAULT_INSTANCE.writeInt(dataOutput, (Integer) objects.get(i));
                            break;
                        case LONG:
                            LongConverter.DEFAULT_INSTANCE.writeLong(dataOutput, (Long) objects.get(i));
                            break;
                        case FLOAT:
                            FloatConverter.DEFAULT_INSTANCE.writeFloat(dataOutput, (Float) objects.get(i));
                            break;
                        case DOUBLE:
                            DoubleConverter.DEFAULT_INSTANCE.writeDouble(dataOutput, (Double) objects.get(i));
                            break;
                        case STRING:
                            StringConverter.DEFAULT_INSTANCE.write(dataOutput, (String) objects.get(i));
                            break;
                        case GEOMETRY:
                            StringConverter.DEFAULT_INSTANCE.write(dataOutput, (String) objects.get(i));
                            break;
                        default:
                            throw new IOException("Unknown data type:" + schema.getAttribute(i).getType());
                    }
                }
            }
        };
    }

    /**
     * Returns a converter for the given attribute
     *
     * @param attribute
     * @return
     */
    public static MeasuredConverter<Object> getObjectConverter(final Attribute attribute) {
        return new MeasuredConverter<Object>() {

            private static final long serialVersionUID = 1L;

            @Override
            public int getMaxObjectSize() {
                return getAttributeSize(attribute);
            }

            @Override
            public Object read(final DataInput dataInput, final Object object) throws IOException {
                switch (attribute.getType()) {
                    case BYTE:
                        return ByteConverter.DEFAULT_INSTANCE.readByte(dataInput);
                    case SHORT:
                        return ShortConverter.DEFAULT_INSTANCE.readShort(dataInput);
                    case INTEGER:
                        return IntegerConverter.DEFAULT_INSTANCE.readInt(dataInput);
                    case LONG:
                        return LongConverter.DEFAULT_INSTANCE.readLong(dataInput);
                    case FLOAT:
                        return FloatConverter.DEFAULT_INSTANCE.readFloat(dataInput);
                    case DOUBLE:
                        return DoubleConverter.DEFAULT_INSTANCE.readDouble(dataInput);
                    case STRING:
                        return StringConverter.DEFAULT_INSTANCE.read(dataInput);
                    case GEOMETRY:
                        return StringConverter.DEFAULT_INSTANCE.read(dataInput);
                    default:
                        throw new IOException("Unknown data type:" + attribute.getType());
                }
            }

            @Override
            public void write(final DataOutput dataOutput, final Object object) throws IOException {
                switch (attribute.getType()) {
                    case BYTE:
                        ByteConverter.DEFAULT_INSTANCE.writeByte(dataOutput, (Byte) object);
                        break;
                    case SHORT:
                        ShortConverter.DEFAULT_INSTANCE.writeShort(dataOutput, (Short) object);
                        break;
                    case INTEGER:
                        IntegerConverter.DEFAULT_INSTANCE.writeInt(dataOutput, (Integer) object);
                        break;
                    case LONG:
                        LongConverter.DEFAULT_INSTANCE.writeLong(dataOutput, (Long) object);
                        break;
                    case FLOAT:
                        FloatConverter.DEFAULT_INSTANCE.writeFloat(dataOutput, (Float) object);
                        break;
                    case DOUBLE:
                        DoubleConverter.DEFAULT_INSTANCE.writeDouble(dataOutput, (Double) object);
                        break;
                    case STRING:
                        StringConverter.DEFAULT_INSTANCE.write(dataOutput, (String) object);
                        break;
                    case GEOMETRY:
                        StringConverter.DEFAULT_INSTANCE.write(dataOutput, (String) object);
                        break;
                    default:
                        throw new IOException("Unknown data type:" + attribute.getType());
                }
            }
        };
    }

    /**
     * Calculates the size of an event object with the given schema
     *
     * @param schema the schema
     * @return The size of an event object
     */
    public static int calculateEventSize(final EventSchema schema, TimeRepresentation rep) {
        int size = rep == TimeRepresentation.INTERVAL ? 16 : 8; // Timestamp(s)
        for (final Attribute attr : schema) {
            size += getAttributeSize(attr);
        }
        return size;
    }

    /**
     * Returns the size of objects of the given attribute
     *
     * @param schema    the schema the attribute refers to
     * @param attribute the attribute´s index
     * @return the size of an object of the given attribute
     */
    public static int getAttributeSize(final EventSchema schema, final int attribute) {
        return getAttributeSize(schema.getAttribute(attribute));
    }

    /**
     * Returns the size of an entry of given attribute
     *
     * @param attribute the attribute which size should be determined
     * @return the size of an entry of the the given attribute in bytes
     */
    public static int getAttributeSize(final Attribute attribute) {
        switch (attribute.getType()) {
            case BYTE:
                return 1;
            case SHORT:
                return 2;
            case INTEGER:
                return 4;
            case LONG:
                return 8;
            case FLOAT:
                return 4;
            case DOUBLE:
                return 8;
            case STRING:
                return attribute.getMaxStringSize() * 4 + 2;
            case GEOMETRY:
                return attribute.getMaxStringSize() * 4 + 2;
            default:
                throw new RuntimeException("Unknown data type: " + attribute.getType());
        }
    }
}
