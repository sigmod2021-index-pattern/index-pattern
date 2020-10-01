package sigmod2021.esp.provider.impl.handler;

import sigmod2021.esp.api.provider.AlreadyRegisteredException;
import sigmod2021.esp.api.provider.NoSuchQueryException;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

public class QueryRegistry implements Iterable<Query> {

    private final Map<String, Query> queries = new HashMap<>();

    public void addQuery(Query handler) throws AlreadyRegisteredException {
        if (hasQuery(handler.getName()))
            throw new AlreadyRegisteredException("There is already a query with name: " + handler.getName()
                    + " and schema: " + handler.getOutputSchema());
        queries.put(handler.getName(), handler);
    }

    public Query getQuery(String name) throws NoSuchQueryException {
        Query result = queries.get(name);
        if (result == null)
            throw new NoSuchQueryException("No query found for name: " + name);
        return result;
    }

    public void removeQuery(String name) throws NoSuchQueryException {
        if (queries.remove(name) == null)
            throw new NoSuchQueryException("No query found for name: " + name);
    }

    public boolean hasQuery(String name) {
        return queries.containsKey(name);
    }

    @Override
    public Iterator<Query> iterator() {
        return queries.values().iterator();
    }

}
