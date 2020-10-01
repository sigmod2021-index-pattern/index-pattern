package sigmod2021.esp.provider.impl.handler;

import sigmod2021.esp.api.provider.AlreadyRegisteredException;
import sigmod2021.esp.api.provider.NoSuchStreamException;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

public class StreamRegistry implements Iterable<StreamHandler> {

    private final Map<String, StreamHandler> streams = new HashMap<>();

    public void addStream(StreamHandler handler) throws AlreadyRegisteredException {
        if (hasStream(handler.getName()))
            throw new AlreadyRegisteredException("There is already a stream with name: " + handler.getName() + " and schema: " + handler.getSchema());
        streams.put(handler.getName(), handler);
    }

    public StreamHandler getStream(String name) throws NoSuchStreamException {
        StreamHandler result = streams.get(name);
        if (result == null)
            throw new NoSuchStreamException("No stream found for name: " + name);
        return result;
    }

    public void removeStream(String name) throws NoSuchStreamException {
        if (streams.remove(name) == null)
            throw new NoSuchStreamException("No stream found for name: " + name);
    }

    public boolean hasStream(String name) {
        return streams.containsKey(name);
    }

    @Override
    public Iterator<StreamHandler> iterator() {
        return streams.values().iterator();
    }

}
