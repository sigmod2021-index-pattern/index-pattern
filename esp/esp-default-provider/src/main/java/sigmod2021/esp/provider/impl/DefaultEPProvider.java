package sigmod2021.esp.provider.impl;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sigmod2021.common.EPException;
import sigmod2021.esp.api.EPProvider;
import sigmod2021.esp.api.bridge.EPBridge;
import sigmod2021.esp.api.bridge.EventChannel;
import sigmod2021.esp.api.epa.*;
import sigmod2021.esp.api.epa.window.Window;
import sigmod2021.esp.api.provider.*;
import sigmod2021.esp.api.util.StreamInfo;
import sigmod2021.esp.provider.impl.handler.*;
import sigmod2021.event.EventSchema;

import java.util.*;

public class DefaultEPProvider implements EPProvider {

    private static final Logger log = LoggerFactory.getLogger(DefaultEPProvider.class);

    private final EPBridge bridge;

    private final StreamRegistry streams = new StreamRegistry();

    private final EPARegistry epas = new EPARegistry();

    private final QueryRegistry queries = new QueryRegistry();


    public DefaultEPProvider(EPBridge bridge) {
        log.info("Creating default provider with bridge: " + bridge.getClass().getName());
        this.bridge = bridge;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public StreamInfo getStreamInfo(String name) throws NoSuchStreamException {
        StreamHandler sh = streams.getStream(name);
        return new StreamInfo(sh.getName(), sh.getSchema());
    }


    @Override
    public StreamHandler registerStream(final String name, EventSchema schema) throws AlreadyRegisteredException, EPException {
        String ucName = name.toUpperCase();
        if (streams.hasStream(ucName))
            throw new AlreadyRegisteredException("There is already a stream with name: " + ucName);

        log.info("Registering stream \"{}\" with schema: {}", ucName, schema);
        EventChannel rs = bridge.registerStream(schema);
        StreamHandler handler = new StreamHandler(ucName, rs);
        streams.addStream(handler);
        return handler;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public StreamHandler registerExternalStream(String name, EventSchema schema) throws AlreadyRegisteredException, EPException {
        return registerStream(name, schema);
    }

    @Override
    public QueryHandle createQuery(String name, EPA epn) throws AlreadyRegisteredException, NoSuchStreamException, EPException {
        if (queries.hasQuery(name))
            throw new AlreadyRegisteredException("There is already a query with name: " + name);

        log.info("Creating query \"{}\" with definition: {}", name, epn.toString());

        Query query = new Query(name, epn);
        createQueryRecursive(epn, query);

        queries.addQuery(query);
        return query;
    }

    private EPAHandler createQueryRecursive(EPA epa, Query query) throws EPException {
        EPAHandler result = null;
        try {
            result = epas.getEPA(epa);
            for (EPA inputEPA : epa.getInputEPAs()) {
                createQueryRecursive(inputEPA, query);
            }
        } catch (NoSuchQueryException nse) {
            if (epa instanceof Stream)
                result = streams.getStream(((Stream) epa).getName());
            else if (epa instanceof Projection)
                result = createProjection((Projection) epa, query);
            else if (epa instanceof Filter)
                result = createFilter((Filter) epa, query);
            else if (epa instanceof Aggregator)
                result = createAggregator((Aggregator) epa, query);
            else if (epa instanceof Correlator)
                result = createCorrelator((Correlator) epa, query);
            else if (epa instanceof PatternMatcher)
                result = createPatternMatcher((PatternMatcher) epa, query);
            else if (epa instanceof Window)
                result = createWindow((Window) epa, query);
            else if (epa instanceof UserDefinedEPA)
                result = createUDO((UserDefinedEPA) epa, query);
            else
                throw new IllegalArgumentException("Unsupported EPA: " + epa);
            epas.addEPA(result);
        }
        query.addEPA(result);
        return result;
    }

    private EPAHandler createWindow(Window window, Query query) throws EPException {
        EPAHandler inputHandler = createQueryRecursive(window.getInputEPAs()[0], query);
        EPAHandler result = new EPAHandler(window);
        result.setResultChannel(bridge.registerWindow(window, result, inputHandler.getResultChannel()));
        return result;
    }

    private EPAHandler createProjection(Projection projection, Query query) throws EPException {
        EPAHandler inputHandler = createQueryRecursive(projection.getInputEPAs()[0], query);
        EPAHandler result = new EPAHandler(projection);
        result.setResultChannel(bridge.registerProjection(projection, result, inputHandler.getResultChannel()));
        return result;
    }

    private EPAHandler createFilter(Filter filter, Query query) throws EPException {
        EPAHandler inputHandler = createQueryRecursive(filter.getInputEPAs()[0], query);
        EPAHandler result = new EPAHandler(filter);
        result.setResultChannel(bridge.registerFilter(filter, result, inputHandler.getResultChannel()));
        return result;
    }

    private EPAHandler createAggregator(Aggregator aggregator, Query query) throws EPException {
        EPAHandler inputHandler = createQueryRecursive(aggregator.getInputEPAs()[0], query);
        EPAHandler result = new EPAHandler(aggregator);
        result.setResultChannel(bridge.registerAggregator(aggregator, result, inputHandler.getResultChannel()));
        return result;
    }

    private EPAHandler createCorrelator(Correlator correlator, Query query) throws EPException {
        EPAHandler leftHandler = createQueryRecursive(correlator.getInputEPAs()[0], query);
        EPAHandler rightHandler = createQueryRecursive(correlator.getInputEPAs()[1], query);
        EPAHandler result = new EPAHandler(correlator);
        result.setResultChannel(bridge.registerCorrelator(correlator, result, leftHandler.getResultChannel(), rightHandler.getResultChannel()));
        return result;
    }

    private EPAHandler createPatternMatcher(PatternMatcher pm, Query query) throws EPException {
        EPAHandler inputHandler = createQueryRecursive(pm.getInputEPAs()[0], query);
        EPAHandler result = new EPAHandler(pm);
        result.setResultChannel(bridge.registerPatternMatcher(pm, result, inputHandler.getResultChannel()));
        return result;
    }

    private EPAHandler createUDO(UserDefinedEPA udo, Query query) throws EPException {
        List<EventChannel> inputStreams = new ArrayList<>();

        for (EPA in : udo.getInputEPAs()) {
            EPAHandler ih = createQueryRecursive(in, query);
            ih.addUserDefinedOperator(udo);
            inputStreams.add(ih.getResultChannel());
        }

        // Create artificial stream
        List<EventSchema> inputSchemas = new ArrayList<EventSchema>();
        for (EventChannel es : inputStreams)
            inputSchemas.add(es.getSchema());

        EventSchema out = udo.computeOutputSchema(inputSchemas.toArray(new EventSchema[inputSchemas.size()]));
        EventChannel rs = bridge.registerStream(out);
        udo.initialize(inputStreams.toArray(new EventChannel[inputStreams.size()]));
        EPAHandler result = new EPAHandler(udo);
        udo.setCallback(result);
        result.setResultChannel(rs);
        return result;
    }


    @Override
    public void registerSink(QueryHandle query, EventSink sink) throws NoSuchQueryException {
        log.info("Adding sink \"{}\" to query \"{}\"", sink, query.getName());
        queries.getQuery(query.getName()).addSink(sink);
    }

    @Override
    public void removeStream(DataSource stream) throws NoSuchStreamException {
        StreamHandler sh = streams.getStream(stream.getName());
        if (sh.getRefCount() > 0)
            throw new IllegalStateException("Cannot remove stream with connected consumers!");

        log.info("Removing stream \"{}\" with schema: {}", sh.getName(), sh.getSchema());

        bridge.removeStream(sh.getResultChannel(), false);
        streams.removeStream(stream.getName());
    }

    @Override
    public void removeQuery(QueryHandle query) throws NoSuchQueryException {
        Query q = queries.getQuery(query.getName());

        log.info("Removing query \"{}\" with definition: {}", q.getName(), q.getDefinition().toString());

        for (EPAHandler eh : q) {
            eh.decreaseRefCount();
            // EPA not used anymore
            if (eh.getRefCount() == 0) {
                removeEPA(eh, false, false);
            }
        }
        queries.removeQuery(query.getName());
    }


    private void removeEPA(EPAHandler eh, boolean force, boolean flush) throws NoSuchQueryException {
        epas.removeEPA(eh.getDefinition());
        if (eh.getDefinition() instanceof UserDefinedEPA) {
            UserDefinedEPA udo = (UserDefinedEPA) eh.getDefinition();
            try {
                bridge.removeStream(eh.getResultChannel(), force);
                for (EPAHandler handler : epas)
                    handler.removeUserDefinedOperator(udo);
                udo.destroy(flush);
            } catch (NoSuchStreamException nse) {
                throw new NoSuchQueryException("No stream registered for UserDefinedOperator: " + udo);
            } catch (EPException je) {
                log.error("Error while destroying user defined operator", je);
            }
        }
        // Default handling
        else if (!(eh.getDefinition() instanceof Stream))
            bridge.removeEPA(eh.getResultChannel(), force, flush);
    }

    @Override
    public void removeSink(QueryHandle query, EventSink sink) throws NoSuchQueryException {
        log.info("Removing sink \"{}\" from query \"{}\"", sink, query.getName());
        queries.getQuery(query.getName()).removeSink(sink);
    }


    @Override
    public void removeSink(EventSink sink) {
        for (Query query : queries)
            query.removeSink(sink);
    }

    @Override
    public void destroy(boolean flush) {
        log.info("Shutting down default provider");
        if (flush)
            destroyFlush();
        else
            destroyNoFlush();
    }


    private void destroyFlush() {
        // Queries
        List<Set<EPAHandler>> leveledEPAs = new ArrayList<>();

        // Collect EPAs in level order
        for (Query q : queries) {
            Iterator<EPAHandler> ei = q.bottomUpIterator();

            for (int i = 0; ei.hasNext(); i++) {
                if (leveledEPAs.size() <= i) {
                    leveledEPAs.add(new HashSet<>());
                }
                leveledEPAs.get(i).add(ei.next());
            }
        }
        // Destory and flush on level order

        for (Set<EPAHandler> ehs : leveledEPAs) {
            for (EPAHandler eh : ehs) {
                try {
                    removeEPA(eh, true, true);
                } catch (NoSuchQueryException nsq) {
                }
            }
        }

        // Streams
        List<StreamHandler> shs = new ArrayList<>();
        for (StreamHandler sh : streams)
            shs.add(sh);

        for (StreamHandler sh : shs) {
            while (sh.getRefCount() > 0)
                sh.decreaseRefCount();
            try {
                removeStream(sh);
            } catch (NoSuchStreamException nss) {
            }
        }

        bridge.destroy();
    }

    private void destroyNoFlush() {
        List<Query> qs = new ArrayList<>();
        List<StreamHandler> shs = new ArrayList<>();

        for (Query q : queries)
            qs.add(q);

        for (StreamHandler sh : streams)
            shs.add(sh);


        for (Query q : qs)
            try {
                removeQuery(q);
            } catch (NoSuchQueryException nsq) {
            }

        for (StreamHandler sh : shs)
            try {
                removeStream(sh);
            } catch (NoSuchStreamException nss) {
            }

        bridge.destroy();
    }
}
