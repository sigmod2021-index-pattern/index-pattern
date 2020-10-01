package sigmod2021.event;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.Serializable;
import java.util.*;
import java.util.stream.Collectors;


public class EventSchema implements Iterable<Attribute>, Serializable {

    private static final byte VERSION_1 = 1;

    public static final byte CURRENT_VERSION = VERSION_1;

    /**
     *
     */
    private static final long serialVersionUID = 1L;

    private final List<Attribute> attributes = new ArrayList<>();

    public EventSchema(Attribute... attributes) throws SchemaException {
        this(Arrays.asList(attributes));
    }

    public EventSchema(List<Attribute> attributes) throws SchemaException {
        // validate
        if (attributes.isEmpty())
            throw new IllegalArgumentException("A schema requires at least one attribute");

        Set<String> names = new HashSet<>();
        for (Attribute a : attributes)
            if (!names.add(a.getQualifiedName()))
                throw new SchemaException("The attribute-names must be unique inside an EventSchema conflict on: " + a.getQualifiedName());

        // Copy the attributes -- for safety, even if they are immutable right
        // now
        for (Attribute a : attributes)
            this.attributes.add(new Attribute(a));
    }

    public static EventSchema read(DataInput in) throws IOException {
        // Read version
        @SuppressWarnings("unused")
        byte version = in.readByte();

        // This is for version 1 -- apply a switch on version increase
        byte numAttribs = in.readByte();

        List<Attribute> attribs = new ArrayList<>(numAttribs);

        for (int i = 0; i < numAttribs; i++) {
            attribs.add(Attribute.read(in));
        }
        try {
            return new EventSchema(attribs.toArray(new Attribute[attribs.size()]));
        } catch (SchemaException e) {
            throw new IOException("Error while deserializing event schema", e);
        }
    }

    public EventSchema copy() {
        return new EventSchema(attributes);
    }

    public EventSchema union(EventSchema other) throws SchemaException {
        return union(other, null, null);
    }

    public EventSchema union(EventSchema other, String qualifierLeft, String qualifierRight) throws SchemaException {
        List<Attribute> result = new ArrayList<>();

        for (Attribute a : attributes) {
            Attribute copy = new Attribute(a);
            if (qualifierLeft != null)
                copy.setQualifier(qualifierLeft);
            result.add(copy);
        }
        for (Attribute a : other.attributes) {
            Attribute copy = new Attribute(a);
            if (qualifierRight != null)
                copy.setQualifier(qualifierRight);
            result.add(copy);
        }
        return new EventSchema(result);
    }

    public int getNumAttributes() {
        return attributes.size();
    }

    public int getAttributeIndex(String name) throws SchemaException {
        int idx = 0;
        String uName = name.toUpperCase();

        // Qualified name
        if (uName.indexOf('.') >= 0) {
            for (Attribute a : attributes)
                if (a.getQualifiedName().equals(uName))
                    return idx;
                else
                    idx++;
        }
        // Ambiguity check
        else if (attributes.stream().filter(x -> x.getName().equals(uName)).collect(Collectors.toList()).size() > 1) {
            throw new SchemaException("Attribute \"" + name + "\" is ambiguous. Use qualified name instead.");
        } else {
            for (Attribute a : attributes)
                if (a.getName().equals(uName))
                    return idx;
                else
                    idx++;
        }
        throw new SchemaException("Attribute \"" + name + "\" not found in schema.");
    }

    public Attribute getAttribute(int index) {
        return attributes.get(index);
    }

    public boolean containsAttribute(String name) {
        try {
            byName(name);
            return true;
        } catch (Exception e) {
            return false;
        }
    }

    public Attribute byName(String name) throws SchemaException {
        String uName = name.toUpperCase();
        // Qualified name
        if (uName.indexOf('.') >= 0) {
            for (Attribute a : attributes)
                if (a.getQualifiedName().equals(uName))
                    return a;
        } else {
            List<Attribute> candidates = attributes.stream().filter(x -> x.getName().equals(uName)).collect(Collectors.toList());
            if (candidates.size() == 1)
                return candidates.get(0);
            else if (candidates.size() > 1)
                throw new SchemaException("Attribute \"" + name + "\" is ambiguous. Use qualified name instead.");
        }
        throw new SchemaException("Attribute \"" + name + "\" not found in schema.");
    }

    public List<String> getAttributeNames() {
        return attributes.stream().map(x -> x.getName()).collect(Collectors.toList());
    }

    public EventSchema setQualifier(String name) {
        for (Attribute a : attributes)
            a.setQualifier(name);
        return this;
    }

    @Override
    public Iterator<Attribute> iterator() {
        return attributes.iterator();
    }

    @Override
    public int hashCode() {
        return ((attributes == null) ? 0 : attributes.hashCode());
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        EventSchema other = (EventSchema) obj;
        if (attributes == null) {
            if (other.attributes != null)
                return false;
        } else if (!attributes.equals(other.attributes))
            return false;
        return true;
    }

    @Override
    public String toString() {
        return "EventSchema: " + attributes;
    }

    public void write(DataOutput out) throws IOException {
        // Write version
        out.writeByte(CURRENT_VERSION);
        // Write number of attributes
        out.writeByte(attributes.size());
        for (Attribute attr : attributes)
            attr.write(out);
    }

}
