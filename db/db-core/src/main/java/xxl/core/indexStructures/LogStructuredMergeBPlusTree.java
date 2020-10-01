package xxl.core.indexStructures;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.PriorityQueue;
import java.util.Queue;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import xxl.core.collections.containers.Container;
import xxl.core.collections.containers.MapContainer;
import xxl.core.cursors.Cursor;
import xxl.core.functions.AbstractFunction;
import xxl.core.functions.Constant;
import xxl.core.indexStructures.BPlusTree.IndexEntry;
import xxl.core.indexStructures.BPlusTree.KeyRange;
import xxl.core.indexStructures.BPlusTree.Node;
import xxl.core.indexStructures.BPlusTree.NodeConverter;
import xxl.core.indexStructures.separators.ComparableKeyRange;
import xxl.core.indexStructures.separators.ComparableSeparator;
import xxl.core.io.converters.MeasuredConverter;
import xxl.core.util.Pair;

/**
 * A LSM implementation using {@link BPlusTree}.
 *
 * @param <KeyType>
 * @param <ValueType>
 */
public class LogStructuredMergeBPlusTree<KeyType extends Comparable<KeyType>, ValueType>
        implements Plottable {

    /**
     * An object containing the configuration for a {@link BPlusTree}.
     */
    public class BPlusTreeConfiguration {
        private final MeasuredConverter<KeyType> keyConverter;
        @SuppressWarnings("unused")
        private final MeasuredConverter<ValueType> valueConverter;
        private final MeasuredConverter<Entry> wrappedValueConverter;
        private final Function<ValueType, KeyType> keyExtractor;
        private final Function<KeyType, Separator> separatorGenerator;
        @SuppressWarnings("unused")
        private final BiFunction<KeyType, KeyType, KeyRange> keyRangeGenerator;
        @SuppressWarnings("deprecation")
        private final xxl.core.functions.Function<Entry, KeyType> wrappedKeyExtractor;
        @SuppressWarnings("deprecation")
        private final xxl.core.functions.Function<KeyType, Separator> wrappedSeparatorGenerator;
        @SuppressWarnings("deprecation")
        private final xxl.core.functions.Function<KeyType, KeyRange> wrappedKeyRangeGenerator;

        private int blockSize = 4096;
        private double splitMinRatio = 0.5;
        private double splitMaxRatio = 1.0;

        private final boolean allowDuplicates = true;

        /**
         * Creates a configuration setting the mandatory fields.
         *
         * @param keyConverter
         * @param valueConverter
         * @param keyExtractor
         * @param separatorGenerator
         * @param keyRangeGenerator
         */
        @SuppressWarnings({"serial", "deprecation"})
        public BPlusTreeConfiguration(MeasuredConverter<KeyType> keyConverter,
                                      MeasuredConverter<ValueType> valueConverter,
                                      Function<ValueType, KeyType> keyExtractor,
                                      Function<KeyType, Separator> separatorGenerator,
                                      BiFunction<KeyType, KeyType, KeyRange> keyRangeGenerator) {
            this.keyConverter = keyConverter;

            this.valueConverter = valueConverter;
            this.wrappedValueConverter = new MeasuredConverter<Entry>() {

                @Override
                public int getMaxObjectSize() {
                    final int sizeOfBoolean = 1;
                    return valueConverter.getMaxObjectSize() + sizeOfBoolean;
                }

                @Override
                public Entry read(DataInput dataInput, Entry object)
                        throws IOException {
                    // object is unused
                    ValueType value = valueConverter.read(dataInput, null);
                    EntryType type = dataInput.readBoolean() ? EntryType.INSERT
                            : EntryType.DELETE;
                    return new Entry(value, type);
                }

                @Override
                public void write(DataOutput dataOutput, Entry object)
                        throws IOException {
                    valueConverter.write(dataOutput, object.value);
                    dataOutput.writeBoolean(object.type == EntryType.INSERT);
                }
            };

            this.keyExtractor = keyExtractor;
            this.wrappedKeyExtractor = new AbstractFunction<Entry, KeyType>() {
                @Override
                public KeyType invoke(Entry argument) {
                    return keyExtractor.apply(argument.value);
                }
            };

            this.separatorGenerator = separatorGenerator;
            this.wrappedSeparatorGenerator = new AbstractFunction<KeyType, Separator>() {
                @Override
                public Separator invoke(KeyType argument) {
                    return separatorGenerator.apply(argument);
                }
            };

            this.keyRangeGenerator = keyRangeGenerator;
            this.wrappedKeyRangeGenerator = new AbstractFunction<KeyType, KeyRange>() {
                @Override
                public KeyRange invoke(KeyType argument0, KeyType argument1) {
                    return keyRangeGenerator.apply(argument0, argument1);
                }
            };
        }

        /**
         * Change the block size.
         *
         * @param blockSize
         * @return the object itself.
         */
        public BPlusTreeConfiguration blockSize(int blockSize) {
            this.blockSize = blockSize;
            return this;
        }

        /**
         * Change the split min ratio.
         *
         * @param splitMinRatio
         * @return the object itself.
         */
        public BPlusTreeConfiguration splitMinRatio(double splitMinRatio) {
            this.splitMinRatio = splitMinRatio;
            return this;
        }

        /**
         * Change the split max ratio.
         *
         * @param splitMaxRatio
         * @return the object itself.
         */
        public BPlusTreeConfiguration splitMaxRatio(double splitMaxRatio) {
            this.splitMaxRatio = splitMaxRatio;
            return this;
        }
    }

    /**
     * Short-hand type for the BPlusTreeConfigurationFactory {@link Function}
     *
     * @param <KeyType>
     * @param <ValueType>
     */
    public interface BPlusTreeConfigurationFactory<KeyType extends Comparable<KeyType>, ValueType>
            extends
            Function<LogStructuredMergeBPlusTree<KeyType, ValueType>, LogStructuredMergeBPlusTree<KeyType, ValueType>.BPlusTreeConfiguration> {
        // Only shortening the type
    }

    /**
     * The entry either contains an insert or a delete.
     */
    private enum EntryType {
        INSERT, DELETE
    }

    /**
     * An LSM entry for the BPlusTree covering insert and delete entries.
     */
    private class Entry {
        private final ValueType value;
        private final EntryType type;

        public Entry(ValueType value, EntryType type) {
            this.value = value;
            this.type = type;
        }

        @Override
        public String toString() {
            return value + (type == EntryType.DELETE ? " (D)" : "");
        }
    }

    private final BPlusTreeConfiguration bPlusTreeConfiguration;
    private final BiFunction<NodeConverter, Integer, Container> containerFactory;

    private final long maxiumFirstLevelSize;
    private long currentFirstLevelSize;

    // these two lists must be in sync.
    private final List<BPlusTree> trees = new ArrayList<>();
    private final List<Container> containers = new ArrayList<>();

    // TODO: bloom filter

    /**
     * Create the LSM.
     *
     * @param containerFactory              A function that maps from a {@link NodeConverter} and the
     *                                      BlockSize to a new {@link Container}.
     * @param bPlusTreeConfigurationFactory This function creates a {@link BPlusTreeConfiguration} by
     *                                      applying this LSM instance.
     * @param maxiumFirstLevelSize          The maximum size of elements of the first level before
     *                                      merging.
     */
    public LogStructuredMergeBPlusTree(
            BiFunction<NodeConverter, Integer, Container> containerFactory,
            BPlusTreeConfigurationFactory<KeyType, ValueType> bPlusTreeConfigurationFactory,
            long maxiumFirstLevelSize) {
        this.bPlusTreeConfiguration = bPlusTreeConfigurationFactory.apply(this);

        this.containerFactory = containerFactory;

        this.maxiumFirstLevelSize = maxiumFirstLevelSize;

        Container container = new MapContainer();
        containers.add(container);
        trees.add(createTree(container,0));
        currentFirstLevelSize = 0;
    }
    
    public static class LSMMeta<K extends Comparable<K>> {
    	
    	public static final String FILENAME = "lsm.mtd";
    	
    	public class LevelInfo {
    		boolean isFilled;
    		int rootParentLevel;
    		long rootId;
    		K minKey;
    		K maxKey;

    		private LevelInfo() {
    		}
    		
			private LevelInfo(BPlusTree tree) {
				this.isFilled = tree != null;
				if ( isFilled ) {
					this.rootParentLevel = tree.rootEntry().parentLevel();
					this.rootId = (Long) tree.rootEntry().id();
					ComparableKeyRange rd = (ComparableKeyRange) tree.rootDescriptor();
					minKey = (K) rd.minBound();
					maxKey = (K) rd.maxBound();
				}
			}
			
			public void write( DataOutput out, MeasuredConverter<K> keyConverter ) throws IOException{
				out.writeBoolean(isFilled);
				if ( isFilled ) {
					out.writeInt(rootParentLevel);
					out.writeLong(rootId);
					keyConverter.write(out, minKey);
					keyConverter.write(out, maxKey);
				}
			}
			
			private void read( DataInput in, MeasuredConverter<K> keyConverter ) throws IOException{
				isFilled = in.readBoolean();
				if ( isFilled ) {
					rootParentLevel = in.readInt();
					rootId = in.readLong();
					minKey = keyConverter.read(in);
					maxKey = keyConverter.read(in);
				}
			}
    	}
    	
    	private List<LevelInfo> levels = new ArrayList<>();
    	
    	
    	public void write( DataOutput out, MeasuredConverter<K> keyConverter ) throws IOException {
    		out.writeInt(levels.size());
    		for ( LevelInfo li : levels )
    			li.write( out, keyConverter );
    	}
    	
    	public void write( Path lsmDir, MeasuredConverter<K> keyConverter ) throws IOException {
    		Path metaFile = lsmDir.resolve(FILENAME);
    		try (DataOutputStream dout = new DataOutputStream( new BufferedOutputStream( Files.newOutputStream(metaFile)) )) {
    			write( dout, keyConverter );
    		}
    	}
    	
    	public static <K extends Comparable<K>> LSMMeta<K> load( Path lsmDir, MeasuredConverter<K> keyConverter ) throws IOException {
    		Path metaFile = lsmDir.resolve(FILENAME);
    		try (DataInputStream din = new DataInputStream( new BufferedInputStream( Files.newInputStream(metaFile)) )) {
    			return LSMMeta.load(din, keyConverter);
    		}
    	}
    	
    	public static <K extends Comparable<K>> LSMMeta<K> load( DataInput in, MeasuredConverter<K> keyConverter ) throws IOException {
    		LSMMeta<K> result = new LSMMeta<K>();
    		int count = in.readInt();
    		for ( int i = 0; i < count; i++ ) {
    			LSMMeta<K>.LevelInfo li = result.new LevelInfo();
    			li.read(in, keyConverter);
    			result.levels.add( li );
    		}
    		return result;
    	}
    	
    	
    }
    
	/**
	 * Creates a new LogStructuredMergeBPlusTree instance
	 */
	public LogStructuredMergeBPlusTree( LSMMeta<KeyType> meta, BiFunction<NodeConverter, Integer, Container> containerLoader, BiFunction<NodeConverter, Integer, Container> containerFactory,
        BPlusTreeConfigurationFactory<KeyType, ValueType> bPlusTreeConfigurationFactory,
        long maxiumFirstLevelSize) {
		
		this(containerFactory,bPlusTreeConfigurationFactory,maxiumFirstLevelSize);
		
		
		int level = 1;
		for ( LSMMeta<KeyType>.LevelInfo li : meta.levels ) {
			BPlusTree tree = new BPlusTree(bPlusTreeConfiguration.blockSize,
                bPlusTreeConfiguration.splitMinRatio,
                bPlusTreeConfiguration.allowDuplicates);
			
			Container container = containerLoader.apply(tree.nodeConverter, level);
			
			// This needs to be done in order to obtain a valid node-converter (otherwise key- and value-converter are null and close crashes)
			tree.initialize(null, null,
                bPlusTreeConfiguration.wrappedKeyExtractor, container,
                bPlusTreeConfiguration.keyConverter,
                bPlusTreeConfiguration.wrappedValueConverter,
                bPlusTreeConfiguration.wrappedSeparatorGenerator,
                bPlusTreeConfiguration.wrappedKeyRangeGenerator,
                new Constant<Double>(bPlusTreeConfiguration.splitMinRatio),
                new Constant<Double>(bPlusTreeConfiguration.splitMaxRatio));
            
                
			if ( li.isFilled ) {
				ComparableKeyRange rootDescriptor = new ComparableKeyRange(li.minKey, li.maxKey);
				
				IndexEntry ie = (IndexEntry) tree.createIndexEntry(li.rootParentLevel);
				ie.initialize(new ComparableSeparator(rootDescriptor.maxBound()));
				ie.initialize(li.rootId);
				
				tree.initialize(ie, rootDescriptor,
	                bPlusTreeConfiguration.wrappedKeyExtractor, container,
	                bPlusTreeConfiguration.keyConverter,
	                bPlusTreeConfiguration.wrappedValueConverter,
	                bPlusTreeConfiguration.wrappedSeparatorGenerator,
	                bPlusTreeConfiguration.wrappedKeyRangeGenerator,
	                new Constant<Double>(bPlusTreeConfiguration.splitMinRatio),
	                new Constant<Double>(bPlusTreeConfiguration.splitMaxRatio));
			}
			else {
				tree = null;
			}
			
			trees.add(tree);
			containers.add(container);
			level++;
		}
	}
	
	
	public LSMMeta<KeyType> close() {
		LSMMeta<KeyType> meta = new LSMMeta<>();
		// Check if cache-level is empty
		if ( trees.get(0).rootDescriptor() != null ) 
			merge();
		for ( int i = 1; i < trees.size(); i++ ) {
			BPlusTree tree = trees.get(i);
			containers.get(i).flush();
			containers.get(i).close();
			LSMMeta<KeyType>.LevelInfo li = meta.new LevelInfo(tree);
			meta.levels.add(li);
		}
		return meta;
	}
	
	public long getLevel0Size() {
		return maxiumFirstLevelSize;
	}
	
	public long getFanOut() {
		return trees.get(0).getIndexNodeB();
	}
	
	public long getNumberOfLeafEntries() {
		return trees.get(0).getLeafNodeB();
	}
	
	public int getMaxLevel() {
		return trees.size()-1;
	}
	
	public boolean levelExists(int level) {
		return level >= 0 && level < trees.size() && trees.get(level) != null;
	}
	
	public Optional<BPlusTree> getTree(int level) {
		return Optional.ofNullable(trees.get(level));
	}

    /**
     * A function that creates a new BPlusTree.
     *
     * @param container Optionally re-use an existing container.
     * @return A BPlusTree.
     */
    @SuppressWarnings("deprecation")
    private BPlusTree createTree(Container container, int level) {
        BPlusTree bPlusTree = new BPlusTree(bPlusTreeConfiguration.blockSize,
                bPlusTreeConfiguration.splitMinRatio,
                bPlusTreeConfiguration.allowDuplicates);

        if (container == null) {
            container = containerFactory.apply(bPlusTree.nodeConverter(),level);
        } else {
            container.clear();
        }

        bPlusTree.initialize(null, null,
                bPlusTreeConfiguration.wrappedKeyExtractor, container,
                bPlusTreeConfiguration.keyConverter,
                bPlusTreeConfiguration.wrappedValueConverter,
                bPlusTreeConfiguration.wrappedSeparatorGenerator,
                bPlusTreeConfiguration.wrappedKeyRangeGenerator,
                new Constant<Double>(bPlusTreeConfiguration.splitMinRatio),
                new Constant<Double>(bPlusTreeConfiguration.splitMaxRatio));

        return bPlusTree;
    }

    /**
     * Calculates how many levels are to be merged before the first empty one.
     *
     * @return the number of levels to merge, beginning with the first one.
     */
    private List<BPlusTree> getMergeLevels() {
        int i = 0;
        while (i < trees.size() && trees.get(i) != null) {
            i++;
        }

        return trees.subList(0, i);
    }

    /**
     * Merges the first levels of LSM into the first that is empty.
     */
    private void merge() {
        List<BPlusTree> mergeLevels = getMergeLevels();
        int levelSize = mergeLevels.size();

        List<Cursor<Entry>> cursors = mergeLevels.stream()
                .map(tree -> (Cursor<Entry>) tree.query()).collect(Collectors.toList());

        Iterator<Entry> mergedEntries;
        Container container;
        if (levelSize == trees.size()) {
            mergedEntries = new EntryMerger(cursors);
            // create new container
            container = null;
        } else {
            mergedEntries = new DeletePreservingEntryMerger(cursors);
            // re-use container
            container = containers.get(levelSize);
        }

        BPlusTree newTree = createTree(container,levelSize);
        if (mergedEntries.hasNext()) {
            new BPlusTreeBulkLoading(newTree, mergedEntries);
        }

        if (levelSize < trees.size()) {
            trees.set(levelSize, newTree);
        } else {
            trees.add(levelSize, newTree);
            containers.add(newTree.container());
        }

        // remove consumed trees
        trees.set(0, createTree(containers.get(0),0));
        for (int i = 1; i < levelSize; i++) {
            trees.set(i, null);
        }

        currentFirstLevelSize = 0;
    }

    /**
     * Inserts a bunch of elements.
     *
     * @param values
     */
    public void insert(Iterator<ValueType> values) {
        values.forEachRemaining(this::insert);
    }

    /**
     * Inserts a single element.
     *
     * @param value
     */
    public void insert(ValueType value) {
        BPlusTree tree = trees.get(0);
        tree.insert(new Entry(value, EntryType.INSERT));
        if (++currentFirstLevelSize >= maxiumFirstLevelSize) {
            merge();
        }
    }

    /**
     * Delete a bunch of elements.
     *
     * @param values
     */
    public void delete(Iterator<ValueType> values) {
        values.forEachRemaining(this::delete);
    }

    /**
     * Delete one element.
     *
     * @param value
     */
    public void delete(ValueType value) {
        BPlusTree tree = trees.get(0);
        KeyType key = bPlusTreeConfiguration.keyExtractor.apply(value);
        Separator separator = bPlusTreeConfiguration.separatorGenerator
                .apply(key);

        @SuppressWarnings("unchecked")
        // Cursor<Entry> query = tree.query(new Entry(value, EntryType.INSERT));
                Cursor<Entry> query = tree.query(separator);
        if (query.hasNext()) {
            query.next();
            query.update(new Entry(value, EntryType.DELETE));
            query.close();
        } else {
            query.close();
            tree.insert(new Entry(value, EntryType.DELETE));
            currentFirstLevelSize++;
        }

        if (currentFirstLevelSize >= maxiumFirstLevelSize) {
            merge();
        }
    }

    /**
     * Exact match query.
     *
     * @param key to match on equality
     * @return value or null if non-existent
     */
    public ValueType query(KeyType key) {
        // query each tree
        @SuppressWarnings("unchecked")
        List<Entry> entries = trees.stream().filter(tree -> tree != null)
                .map(tree -> (Entry) tree.exactMatchQuery(key))
                .filter(result -> result != null).collect(Collectors.toList());

        for (Entry entry : entries) {
            return entry.type == EntryType.INSERT ? entry.value : null;
        }

        return null; // nothing found at all
    }

    /**
     * Range query.
     *
     * @param rangeFrom key inclusive
     * @param rangeTo   key inclusive
     * @return Iterator of values.
     */
    public Iterator<ValueType> query(KeyType rangeFrom, KeyType rangeTo) {
        List<Cursor<Entry>> entryCursors = trees.stream()
                .filter(tree -> tree != null)
                .map(tree -> (Cursor<Entry>) tree.rangeQuery(rangeFrom, rangeTo))
                .collect(Collectors.toList());
        return new EntryToValueMapper(new EntryMerger2(entryCursors));
    }

    /**
     * Range query over the whole range, .i.e. all data.
     *
     * @return Iterator of values
     */
    public Iterator<ValueType> query() {
        List<Cursor<Entry>> entryCursors = trees.stream()
                .filter(tree -> tree != null).map(tree -> (Cursor<Entry>) tree.query())
                .collect(Collectors.toList());
        return new EntryToValueMapper(new EntryMerger2(entryCursors));
    }

    private class EntryToValueMapper implements Iterator<ValueType> {

        private final Iterator<Entry> iterator;

        public EntryToValueMapper(Iterator<Entry> iterator) {
            this.iterator = iterator;
        }

        @Override
        public boolean hasNext() {
            return iterator.hasNext();
        }

        @Override
        public ValueType next() {
            return iterator.next().value;
        }
    }
    
    
    private class TreeBuffer implements Iterator<Entry> {
    	private static final int BUFFER_SIZE = 10_000; 
		private final int idx;
		private final Cursor<Entry> cursor;
		private final ArrayDeque<Entry> buffer;
		
		public TreeBuffer(int idx, Cursor<LogStructuredMergeBPlusTree<KeyType, ValueType>.Entry> cursor) {
			this.idx = idx;
			this.cursor = cursor;
			this.buffer = new ArrayDeque<>(BUFFER_SIZE);
		}
		
		private void fillBuffer() {
			for ( int remaining = BUFFER_SIZE-buffer.size(); remaining > 0 && cursor.hasNext(); remaining-- ) {
				buffer.add(cursor.next());
			}
		}

		/**
		 * @{inheritDoc}
		 */
		@Override
		public boolean hasNext() {
			return !buffer.isEmpty() || cursor.hasNext();
		}

		/**
		 * @{inheritDoc}
		 */
		@Override
		public LogStructuredMergeBPlusTree<KeyType, ValueType>.Entry next() {
			if ( !buffer.isEmpty() )
				return buffer.poll();
			else if ( cursor.hasNext() ) {
				fillBuffer();
				return buffer.poll();
			}
			return null;
		}
	}
    
    private class EntryMerger2 implements Iterator<Entry> {

    	private final List<TreeBuffer> buffers = new ArrayList<>();
    	
    	private final List<Entry> entries;
    	
    	public EntryMerger2( List<Cursor<Entry>> cursors ) {
    		entries = new ArrayList<>();
    		for ( int i = 0; i < cursors.size(); i++ ) {
    			TreeBuffer b = new TreeBuffer(i, cursors.get(i));
    			buffers.add( b );
    			entries.add( b.next() );
    		}
    	}

		/**
		 * @{inheritDoc}
		 */
		@Override
		public boolean hasNext() {
			for ( Entry e : entries )
				if ( e != null )
					return true;
			return false;
		}

		/**
		 * @{inheritDoc}
		 */
		@Override
		public LogStructuredMergeBPlusTree<KeyType, ValueType>.Entry next() {
			KeyType minKey = null;
			
			int minIdx     = -1;
			
			// TODO Delete Handling
			
			for ( int i = 0; i < entries.size(); i++ ) {
				Entry e = entries.get(i);
				if ( e == null )
					continue;
				
				KeyType k = bPlusTreeConfiguration.keyExtractor.apply(e.value);
				
				if ( minKey == null || k.compareTo(minKey) < 0 ) {
					minKey = k;
					minIdx = i;
				}
				
			}
			
			if ( minIdx >= 0 ) {
				var result = entries.get(minIdx);
				entries.set(minIdx, buffers.get(minIdx).next());
				return result;
			}
			else {
				return null;
			}
		}
    	
    }
    
    

    private class EntryMerger implements Iterator<Entry> {

        private Comparator<Pair<Cursor<Entry>, Integer>> comparator = new Comparator<Pair<Cursor<Entry>, Integer>>() {

            @Override
            public int compare(Pair<Cursor<Entry>, Integer> o1,
                               Pair<Cursor<Entry>, Integer> o2) {
                KeyType key1 = bPlusTreeConfiguration.keyExtractor.apply(o1
                        .getFirst().peek().value);
                KeyType key2 = bPlusTreeConfiguration.keyExtractor.apply(o2
                        .getFirst().peek().value);
                int comparison = key1.compareTo(key2);
                return comparison != 0 ? comparison : Integer.compare(
                        o1.getSecond(), o2.getSecond());
            }
        };

        protected final PriorityQueue<Pair<Cursor<Entry>, Integer>> cursorHeap;

        protected KeyType currentDeleteKey = null;
        protected Entry nextEntry = null;

        public EntryMerger(List<Cursor<Entry>> cursors) {
            cursorHeap = new PriorityQueue<>(cursors.size(), comparator);
            for (int i = 0; i < cursors.size(); i++) {
                Cursor<Entry> cursor = cursors.get(i);
                if (cursor.hasNext()) {
                    cursorHeap.add(new Pair<>(cursor, i));
                }
            }
        }

        @Override
        public boolean hasNext() {
            while (nextEntry == null && cursorHeap.size() > 0) {
                nextEntry = nextEntry();
            }

            return nextEntry != null;
        }

        protected Entry retrieveEntryFromHeap() {
            // retrieve cursor with min entry
            Pair<Cursor<Entry>, Integer> element = cursorHeap.poll();
            Cursor<Entry> cursor = element.getFirst();

            Entry entry = cursor.next();

            // put cursor back on heap if elements still exist
            if (cursor.hasNext()) {
                cursorHeap.add(element);
            }

            return entry;
        }

        protected Entry nextEntry() {
            Entry entry = retrieveEntryFromHeap();

            if (entry.type == EntryType.DELETE) {
                currentDeleteKey = bPlusTreeConfiguration.keyExtractor
                        .apply(entry.value);

                return null;

            } else if (currentDeleteKey != null
                    && currentDeleteKey
                    .equals(bPlusTreeConfiguration.keyExtractor
                            .apply(entry.value))) {
                return null;
            } else {
                return entry;
            }
        }

        @Override
        public Entry next() {
            Entry result = nextEntry;
            nextEntry = null;
            return result;
        }
    }

    private class DeletePreservingEntryMerger extends EntryMerger {

        public DeletePreservingEntryMerger(List<Cursor<Entry>> cursors) {
            super(cursors);
        }

        protected Entry currentDeleteEntry = null;
        protected boolean deleteMatched;

        protected Entry temporaryStoredEntry = null;

        @Override
        public boolean hasNext() {
            if (temporaryStoredEntry != null && nextEntry == null) {
                nextEntry = temporaryStoredEntry;
                temporaryStoredEntry = null;
            }

            while (nextEntry == null && cursorHeap.size() > 0) {
                nextEntry = nextEntry();
            }

            if (nextEntry == null && !deleteMatched) {
                nextEntry = currentDeleteEntry;
                // do not call this branch twice
                deleteMatched = true;
            }

            return nextEntry != null;
        }

        @Override
        protected Entry nextEntry() {
            Entry entry = retrieveEntryFromHeap();

            if (entry.type == EntryType.DELETE) {
                // store delete entry temporary

                Entry result = null;

                if (currentDeleteEntry != null && !deleteMatched) {
                    // if delete did not match, return it
                    result = currentDeleteEntry;
                }

                currentDeleteEntry = entry;
                currentDeleteKey = bPlusTreeConfiguration.keyExtractor
                        .apply(entry.value);
                deleteMatched = false;

                return result;

            } else if (currentDeleteKey != null) {
                KeyType entryKey = bPlusTreeConfiguration.keyExtractor
                        .apply(entry.value);

                int comparison = currentDeleteKey.compareTo(entryKey);
                // < 0 must never happen

                if (comparison == 0) {
                    // reject insert entry because delete entry exists
                    deleteMatched = true;

                    return null;
                } else {

                    Entry result;

                    if (deleteMatched) {
                        // entries cancelled each other out
                        result = entry;
                    } else {
                        temporaryStoredEntry = entry;
                        result = currentDeleteEntry;
                    }

                    currentDeleteEntry = null;
                    currentDeleteKey = null;

                    return result;
                }

            } else {
                // pass insert entry

                return entry;
            }
        }
    }

    @Override
    public String getPlot() {
        StringBuilder buffer = new StringBuilder();
        buffer.append("digraph LSM { \n");
        buffer.append("ratio=\"compress\";\n size=\"11.7,8.3!\";\n margin=0;\n");
        buffer.append("graph [rankdir=TB];\n node [shape=record];\n labelloc=\"t\"; \n");

        appendPlot(buffer, "");

        buffer.append("}");
        return buffer.toString();
    }

    @Override
    public void appendPlot(StringBuilder buffer, String prefix) {
        Supplier<String> nameGenerator = Plottable.getNameGenerator(prefix);

        for (int i = 0; i < trees.size(); i++) {
            BPlusTree tree = trees.get(i);

            buffer.append(String.format(
                    "subgraph cluster%1$s { %n label = \"Tree #%2$d\"; %n",
                    nameGenerator.get(), i));
            if (tree == null || tree.rootEntry() == null) {
                buffer.append(String.format("%1$s [label=\"empty\"]; %n",
                        nameGenerator.get()));
            } else {
                // TODO: exchange if BPlusTree implements interface
                appendPlot(tree, buffer, prefix + "t" + i);
            }
            buffer.append("} \n");
        }

    }

    @SuppressWarnings("unchecked")
    private void appendPlot(BPlusTree tree, StringBuilder buffer, String prefix) {
        Supplier<String> nameGenerator = Plottable.getNameGenerator(prefix);

        String rootName = nameGenerator.get();
        buffer.append(String.format("%1$s [label=\"{%2$s|%3$s}\"]; \n",
                rootName, Plottable.escapeText(tree.rootEntry().toString()),
                Plottable.escapeText(tree.rootDescriptor().toString())));

        Queue<Pair<String, IndexEntry>> entries = new ArrayDeque<>(
                Arrays.asList(new Pair<>(rootName, (IndexEntry) tree.rootEntry())));
        while (!entries.isEmpty()) {
            Pair<String, IndexEntry> entry = entries.poll();
            Node node = (Node) entry.getSecond().get();

            String name = nameGenerator.get();

            buffer.append(name);
            buffer.append(" [label=\"");

            // id
            if (node.level > 0) {

            }
            if (node.level > 0) {
                for (IndexEntry indexEntry : (List<IndexEntry>) node.entries) {
                    buffer.append(Plottable.escapeText(indexEntry.separator()
                            .toString()));
                    buffer.append(" | ");
                }
                // remove last |
                if (node.entries.size() > 0) {
                    buffer.setLength(buffer.length() - 2);
                }

            } else {

                for (Object value : node.entries) {
                    buffer.append(Plottable.escapeText(value.toString()));
                    buffer.append("\\n ");
                }

            }

            buffer.append("\"]; \n");

            // link
            buffer.append(String.format("%1$s -> %2$s; %n", entry.getFirst(),
                    name));

            if (node.level > 0) {
                for (IndexEntry indexEntry : (List<IndexEntry>) node.entries) {
                    entries.add(new Pair<>(name, indexEntry));
                }
            }

        }
    }
}
