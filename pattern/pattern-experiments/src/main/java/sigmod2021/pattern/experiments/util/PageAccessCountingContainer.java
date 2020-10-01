package sigmod2021.pattern.experiments.util;

import xxl.core.collections.containers.Container;
import xxl.core.collections.containers.io.SuspendableContainer;
import xxl.core.cursors.Cursor;
import xxl.core.cursors.Cursors;
import xxl.core.functions.Function;
import xxl.core.io.converters.FixedSizeConverter;

import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.concurrent.atomic.AtomicLong;


/**
 *
 */
public class PageAccessCountingContainer implements SuspendableContainer {

    private final Container underlying;
    protected AtomicLong readCount;
    protected AtomicLong writeCount;

    /**
     * Creates a new PageAccessCountingContainer instance
     * @param underlying
     */
    public PageAccessCountingContainer(Container underlying) {
        this.underlying = underlying;
        this.readCount = new AtomicLong();
        this.writeCount = new AtomicLong();
    }

    public void resetCounters() {
        resetReadCount();
        resetWriteCount();
    }

    public void resetReadCount() {
        readCount.set(0);
    }

    public void resetWriteCount() {
        writeCount.set(0);
    }

    public long getReadCount() {
        return readCount.get();
    }

    public long getWriteCount() {
        return writeCount.get();
    }

    /**
     * @{inheritDoc}
     */
    @Override
    public void clear() {
        underlying.clear();
    }

    /**
     * @{inheritDoc}
     */
    @Override
    public void close() {
        underlying.close();
    }

    /**
     * @{inheritDoc}
     */
    @Override
    public boolean contains(Object id) {
        return underlying.contains(id);
    }

    /**
     * @{inheritDoc}
     */
    @Override
    public void flush() {
        underlying.flush();
    }

    /**
     * @{inheritDoc}
     */
    @Override
    public void flush(Object id) {
        underlying.flush(id);
    }

    /**
     * @{inheritDoc}
     */
    @Override
    public Object get(Object id, boolean unfix) throws NoSuchElementException {
        readCount.incrementAndGet();
        return underlying.get(id, unfix);
    }

    /**
     * @{inheritDoc}
     */
    @Override
    public Object get(Object id) throws NoSuchElementException {
        readCount.incrementAndGet();
        return underlying.get(id);
    }

    /**
     * @{inheritDoc}
     */
    @Override
    public Iterator getAll(Iterator ids, boolean unfix) throws NoSuchElementException {
        return underlying.getAll(new CountingIterator<>(ids, readCount), unfix);
    }

    /**
     * @{inheritDoc}
     */
    @Override
    public Iterator getAll(Iterator ids) throws NoSuchElementException {
        return underlying.getAll(new CountingIterator<>(ids, readCount));
    }

    /**
     * @{inheritDoc}
     */
    @Override
    public Iterator ids() {
        return underlying.ids();
    }

    /**
     * @{inheritDoc}
     */
    @Override
    public Object insert(Object object, boolean unfix) {
        writeCount.incrementAndGet();
        return underlying.insert(object, unfix);
    }

    /**
     * @{inheritDoc}
     */
    @Override
    public Object insert(Object object) {
        writeCount.incrementAndGet();
        return underlying.insert(object);
    }

    /**
     * @{inheritDoc}
     */
    @Override
    public Iterator insertAll(Iterator objects, boolean unfix) {
        return underlying.insertAll(new CountingIterator<>(objects, writeCount), unfix);
    }

    /**
     * @{inheritDoc}
     */
    @Override
    public Iterator insertAll(Iterator objects) {
        return underlying.insertAll(new CountingIterator<>(objects, writeCount));
    }

    /**
     * @{inheritDoc}
     */
    @Override
    public boolean isUsed(Object id) {
        return underlying.isUsed(id);
    }

    /**
     * @{inheritDoc}
     */
    @Override
    public Cursor objects() {
        return Cursors.wrap(new CountingIterator<>(underlying.objects(), writeCount));
    }

    /**
     * @{inheritDoc}
     */
    @Override
    public FixedSizeConverter objectIdConverter() {
        return underlying.objectIdConverter();
    }

    /**
     * @{inheritDoc}
     */
    @Override
    public int getIdSize() {
        return underlying.getIdSize();
    }

    /**
     * @{inheritDoc}
     */
    @Override
    public void remove(Object id) throws NoSuchElementException {
        underlying.remove(id);
    }

    /**
     * @{inheritDoc}
     */
    @SuppressWarnings("rawtypes")
    @Override
    public void removeAll(Iterator ids) throws NoSuchElementException {
        underlying.removeAll(ids);
    }

    /**
     * @{inheritDoc}
     */
    @Override
    public Object reserve(Function getObject) {
        return underlying.reserve(getObject);
    }

    /**
     * @{inheritDoc}
     */
    @Override
    public int size() {
        return underlying.size();
    }

    /**
     * @{inheritDoc}
     */
    @Override
    public void unfix(Object id) throws NoSuchElementException {
        underlying.unfix(id);
    }

    /**
     * @{inheritDoc}
     */
    @SuppressWarnings("rawtypes")
    @Override
    public void unfixAll(Iterator ids) throws NoSuchElementException {
        underlying.unfixAll(ids);
    }

    /**
     * @{inheritDoc}
     */
    @Override
    public void update(Object id, Object object, boolean unfix) throws NoSuchElementException {
        writeCount.incrementAndGet();
        underlying.update(id, object, unfix);
    }

    /**
     * @{inheritDoc}
     */
    @Override
    public void update(Object id, Object object) throws NoSuchElementException {
        writeCount.incrementAndGet();
        underlying.update(id, object);
    }

    /**
     * @{inheritDoc}
     */
    @Override
    public void updateAll(Iterator ids, Function function, boolean unfix) throws NoSuchElementException {
        underlying.updateAll(new CountingIterator<>(ids, writeCount), function, unfix);
    }

    /**
     * @{inheritDoc}
     */
    @Override
    public void updateAll(Iterator ids, Function function) throws NoSuchElementException {
        underlying.updateAll(new CountingIterator<>(ids, writeCount), function);
    }

    /**
     * @{inheritDoc}
     */
    @Override
    public void updateAll(Iterator ids, Iterator objects, boolean unfix) throws NoSuchElementException {
        underlying.updateAll(new CountingIterator<>(ids, writeCount), objects, unfix);
    }

    /**
     * @{inheritDoc}
     */
    @Override
    public void updateAll(Iterator ids, Iterator objects) throws NoSuchElementException {
        underlying.updateAll(new CountingIterator<>(ids, writeCount), objects);
    }

    /**
     * @{inheritDoc}
     */
    @Override
    public Object[] batchInsert(Object[] blocks) {
        writeCount.addAndGet(blocks.length);
        return underlying.batchInsert(blocks);
    }

    /**
     * @{inheritDoc}
     */
    @Override
    public void suspend() {
        if (underlying instanceof SuspendableContainer)
            ((SuspendableContainer) underlying).suspend();
        else
            throw new UnsupportedOperationException("Underlying container is not suspendable.");
    }

    /**
     * @{inheritDoc}
     */
    @Override
    public void resume() {
        if (underlying instanceof SuspendableContainer)
            ((SuspendableContainer) underlying).resume();
        else
            throw new UnsupportedOperationException("Underlying container is not suspendable.");
    }

    /**
     * @{inheritDoc}
     */
    @Override
    public boolean isSuspended() {
        if (underlying instanceof SuspendableContainer)
            return ((SuspendableContainer) underlying).isSuspended();
        else
            throw new UnsupportedOperationException("Underlying container is not suspendable.");
    }

    /**
     * @{inheritDoc}
     */
    @Override
    public Iterator<?> idsBackwards() {
        if (underlying instanceof SuspendableContainer)
            return ((SuspendableContainer) underlying).idsBackwards();
        else
            throw new UnsupportedOperationException("Underlying container is not suspendable.");
    }

    /**
     * @{inheritDoc}
     */
    @Override
    public double getCompressionRatio() {
        if (underlying instanceof SuspendableContainer)
            return ((SuspendableContainer) underlying).getCompressionRatio();
        else
            return 1.0;
    }

    /**
     * @{inheritDoc}
     */
    @Override
    public long getAllocatedSpace() {
        if (underlying instanceof SuspendableContainer)
            return ((SuspendableContainer) underlying).getAllocatedSpace();
        else
            throw new UnsupportedOperationException("Underlying container is not suspendable.");
    }

    /**
     * @{inheritDoc}
     */
    @Override
    public void allocateSpace(long space) {
        if (underlying instanceof SuspendableContainer)
            ((SuspendableContainer) underlying).allocateSpace(space);
        else
            throw new UnsupportedOperationException("Underlying container is not suspendable.");

    }

    private class CountingIterator<T> implements Iterator<T> {

        private final Iterator<T> underlying;

        private final AtomicLong counter;

        /**
         * Creates a new CountingIterator instance
         * @param underlying
         * @param counter
         */
        public CountingIterator(Iterator<T> underlying, AtomicLong counter) {
            this.underlying = underlying;
            this.counter = counter;
        }

        /**
         * @{inheritDoc}
         */
        @Override
        public boolean hasNext() {
            return underlying.hasNext();
        }

        /**
         * @{inheritDoc}
         */
        @Override
        public T next() {
            T result = underlying.next();
            counter.incrementAndGet();
            return result;
        }
    }

}
