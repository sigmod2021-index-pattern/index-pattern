package sigmod2021.esp.api.util;

import sigmod2021.event.EventSchema;

import java.io.Serializable;

/**
 *
 */
public class StreamInfo implements Serializable {

    /**
     *
     */
    private static final long serialVersionUID = 1L;

    private final String name;

    private final EventSchema schema;

    public StreamInfo(String name, EventSchema schema) {
        this.name = name;
        this.schema = schema;
    }

    public String getName() {
        return name;
    }

    public EventSchema getSchema() {
        return schema;
    }


    @Override
    public String toString() {
        return "StreamInfo [name=" + name + ", schema=" + schema + "]";
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((name == null) ? 0 : name.hashCode());
        result = prime * result + ((schema == null) ? 0 : schema.hashCode());
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
        StreamInfo other = (StreamInfo) obj;
        if (name == null) {
            if (other.name != null)
                return false;
        } else if (!name.equals(other.name))
            return false;
        if (schema == null) {
            if (other.schema != null)
                return false;
        } else if (!schema.equals(other.schema))
            return false;
        return true;
    }


}
