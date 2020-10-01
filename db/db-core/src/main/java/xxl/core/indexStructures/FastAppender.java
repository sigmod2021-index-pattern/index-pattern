package xxl.core.indexStructures;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.Queue;
import java.util.SortedSet;
import java.util.Stack;
import java.util.TreeSet;
import java.util.concurrent.Future;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.locks.ReentrantLock;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import sigmod2021.db.core.primaryindex.impl.InsertFuture;
import sigmod2021.db.core.wal.Log;
import sigmod2021.db.core.wal.NoLog;
import sigmod2021.db.event.Persistent;
import sigmod2021.db.event.TID;
import sigmod2021.db.queries.NoSuchEventException;
import xxl.core.collections.MapEntry;
import xxl.core.collections.containers.AbstractContainer;
import xxl.core.collections.containers.Container;
import xxl.core.cursor.DoubleCursor;
import xxl.core.cursor.EmptyDoubleCursor;
import xxl.core.functions.AbstractFunction;
import xxl.core.functions.Function;
import xxl.core.functions.Functional.BinaryFunction;
import xxl.core.indexStructures.BPlusLink.IndexEntry;
import xxl.core.indexStructures.BPlusLink.Node;
import xxl.core.indexStructures.appender.LeafLevelCursor;
import xxl.core.io.Buffer;
import xxl.core.io.LRUBuffer;
import xxl.core.io.converters.FixedSizeConverter;
import xxl.core.util.Pair;

/**
 * Fast tree appender, writing buffer nodes with consecutively.
 *
 */
@SuppressWarnings("deprecation")
public abstract class FastAppender<TIN, TOUT extends Persistent<TIN>> implements Appender<TIN,TOUT> {

	/**
	 * The logger.
	 */
	private static final Logger	logger		= LoggerFactory.getLogger(FastAppender.class);

	/**
	 * The level of a leaf node.
	 */
	protected static final int	LEAF_LEVEL	= 0;

	/**
	 * Encapsulates a node and its ID.
	 */
	protected class BufferEntry {

		long	id;
		Node	node;
		Long 	nextId;
		
		public BufferEntry(final long id, final Node node) {
			this(id, node, null);
		}
		
		public BufferEntry(final long id, final Node node, final Long nextId) {
			this.node = node;
			this.nextId = nextId;
			this.id = id;
		}

		@Override
		public String toString() {
			return this.node.toString();
		}
	}
	
	protected class BufferWriteEntry extends BufferEntry {
		
		public BufferWriteEntry(final long id, final Node node) {
			this(id, node, null);
		}
		
		public BufferWriteEntry(final long id, final Node node, final Long nextId) {
			super(id,node,nextId);
		}
		
	}

	// =================================================================================================================

	/** The current size of the tree */
	private int										treeSize;
	/** The buffer */
	protected AppenderBuffer						buffer;
	/** The container for the appender */
	protected AppenderContainer						appenderContainer;
	/** The tree */
	protected BPlusLink								tree;
	/** The maximum number of leaf entries in the tree */
	protected int									maxLeafEntries;
	/** The maximum number of leaf entries */
	protected int									maxIndexEntries;
	/** The first entry of the tree */
	private TIN										first;
	/** The last entry of the tree */
	private TIN										last;
	/////////////////////////////////////////////////////////////
	/////////////// Logging / Out-of-order insertions////////////
	/////////////////////////////////////////////////////////////
	/* Lock for buffer accesses */
	protected ReentrantLock							bufferLock;
	/** The WAL */
	protected Log<TIN> wal;
	/** Mirror for outOfOrderQueue */
	protected Log<TIN>								outOfOrderQueueDiskMirror;
	/** Queue for out-of-order inserts */
	protected Queue<TIN>								outOfOrderQueue;
	/** The maximum size of the out-of-order queue */
	protected int									outOfOrderQueueSize;
	/** The size of the LRU tree buffer */
	protected int									treeBufferSize;

	protected java.util.function.Function<TIN, Long>	keyFunction;
	
	protected final BinaryFunction<TID, TIN, TOUT>		generateOut;

	/**
	 * Creates a new appender for the given tree. Therefore, the appender
	 * replaces the tree`s container.
	 *
	 * @param tree
	 *            the tree
	 * @param bufferSize
	 *            the size of the output buffer
	 * @param idStepSize
	 *            the id step size of the underlying container
	 */
	public FastAppender(final BPlusLink tree, final int bufferSize, final int idStepSize, final Log<TIN> log,
			final Log<TIN> outOfOrderQueueDiskMirror, final float spare, final int outOfOrderQueueSize,
			final int treeBufferSize, final java.util.function.Function<TIN, Long> getKeyFunction, BinaryFunction<TID,TIN,TOUT> generateOut) {
		this(tree, bufferSize, idStepSize, treeBufferSize, generateOut);
		this.wal = log;
		this.outOfOrderQueueDiskMirror = outOfOrderQueueDiskMirror;
		this.maxIndexEntries = (int) (tree.getIndexNodeB() * (1.0f - spare));
		this.maxLeafEntries = (int) (tree.getLeafNodeB() * (1.0f - spare));
		this.outOfOrderQueueSize = outOfOrderQueueSize;
		if ( tree.rootEntry() != null ) { // Try to load the data
			this.buffer.load();
		}
		this.keyFunction = getKeyFunction;
		this.outOfOrderQueue = new PriorityBlockingQueue<>(outOfOrderQueueSize, new Comparator<TIN>() {

			@Override
			public int compare(final TIN o1, final TIN o2) {
				return (getKeyFunction.apply(o1).compareTo(getKeyFunction.apply(o2)));
			}
		});
		logger.info("Spare for appender: " + (spare * 100) + "%");
		logger.info("Events per leaf node: " + this.maxLeafEntries);
	}

	/**
	 * Creates an appender for recovery.
	 *
	 * @param tree
	 *            the tree
	 * @param bufferSize
	 *            the output buffer size
	 * @param treePath
	 *            the right flank of the tree
	 * @param first
	 *            the first entry
	 * @param last
	 *            the last entry
	 */
	public FastAppender(final BPlusLink tree, final int bufferSize, final int idStepSize, final float spare, BinaryFunction<TID,TIN,TOUT> generateOut,
			final List<Pair<Long, ? extends Node>> treePath, final TIN first, final TIN last) {
		this(tree, bufferSize, idStepSize, spare, generateOut, treePath, first, last, 400);
	}

	/**
	 * Creates an appender for recovery.
	 *
	 * @param tree
	 *            the tree
	 * @param bufferSize
	 *            the output buffer size
	 * @param treePath
	 *            the right flank of the tree
	 * @param first
	 *            the first entry
	 * @param last
	 *            the last entry
	 * @param treeBufferSize
	 *            the size of the tree buffer
	 */
	public FastAppender(final BPlusLink tree, final int bufferSize, final int idStepSize, final float spare,
			BinaryFunction<TID,TIN,TOUT> generateOut,
			final List<Pair<Long, ? extends Node>> treePath, final TIN first, final TIN last,
			final int treeBufferSize) {
		this(tree, bufferSize, idStepSize, treeBufferSize, generateOut);
		this.maxIndexEntries = (int) (tree.getIndexNodeB() * (1.0f - spare));
		this.maxLeafEntries = (int) (tree.getLeafNodeB() * (1.0f - spare));
//		this.maxIndexEntries = tree.getIndexNodeB();
//		this.maxLeafEntries = tree.getLeafNodeB();
		this.first = first;
		this.last = last;
		/* ADDED MK */
		this.outOfOrderQueueSize = 1;
		this.outOfOrderQueue = new PriorityBlockingQueue<>(this.outOfOrderQueueSize, new Comparator<TIN>() {

			@Override
			public int compare(final TIN o1, final TIN o2) {
				final Long k1 = (Long) tree.getKey.invoke(o1);
				final Long k2 = (Long) tree.getKey.invoke(o2);
				return Long.compare(k1, k2);
			}
		});
		this.wal = new NoLog<>();
		this.outOfOrderQueueDiskMirror = new NoLog<>();
		/* END ADDED MK */
		
		for (int i = 0; i < treePath.size(); i++) {
			this.buffer.treePath.add(i, new BufferEntry(treePath.get(i).getElement1(), treePath.get(i).getElement2()));
		}
		this.treeSize = treePath.size();
		// Remove container IDs
//		this.appenderContainer.remove(this.buffer.treePath.get(0).id);
	}

	/**
	 * Creates a new appender for the given tree.
	 *
	 * @param tree
	 *            the tree
	 * @param bufferSize
	 *            the size of the buffer
	 * @param idStepSize
	 *            the container id step size
	 */
	protected FastAppender(final BPlusLink tree, final int bufferSize, final int idStepSize, BinaryFunction<TID,TIN,TOUT> generateOut) {
		this(tree, bufferSize, idStepSize, 400, generateOut);
	}

	/**
	 * Creates a new appender for the given tree.
	 *
	 * @param tree
	 *            the tree
	 * @param bufferSize
	 *            the size of the buffer
	 * @param idStepSize
	 *            the container id step size
	 * @param treeBufferSize
	 *            the size of the tree buffer
	 */
	protected FastAppender(final BPlusLink tree, final int bufferSize, final int idStepSize, final int treeBufferSize, BinaryFunction<TID,TIN,TOUT> generateOut) {
		this.tree = tree;
		this.appenderContainer = new AppenderContainer(tree.container());
		this.buffer = new AppenderBuffer(this.appenderContainer, bufferSize, idStepSize);
		this.appenderContainer.setBuffer(this.buffer, new LRUBuffer<Object, Long, Object>(treeBufferSize));
		tree.setContainer(this.appenderContainer);
		this.bufferLock = new ReentrantLock(false);
		this.treeBufferSize = treeBufferSize;
		this.generateOut = generateOut;
	}

	/**
	 * Returns the container.
	 *
	 * @return the container
	 */
	public Container container() {
		return this.buffer.container;
	}

	/**
	 * Returns the tree.
	 *
	 * @return the tree
	 */
	public BPlusTree getTree() {
		return this.tree;
	}

	/**
	 * Inserts the given entry into the tree
	 *
	 * @param entry
	 *            the entry to be inserted
	 */
	@Override
	public Future<TID> insertEntry(final TIN entry) {
		if ( this.first == null ) {
			this.first = entry;
		}
		this.last = entry;
		
		insert(entry, LEAF_LEVEL);

		long blockId = 0;
		int offset = 0;

		if ( this.buffer.treePath.size() > 0 ) {
			BufferEntry current = this.buffer.treePath.get(0);
			blockId = current.id;
			offset = current.node.entries.size()-1;
		}
		
		// TODO: Implement Future Handling
		// Important: the returned event id may be not stable because of
		// out-of-order inserts in the meantime! For stable IDs, use the listener
		// option to get informed about push-outs.
		return new InsertFuture(new TID(blockId,offset));
	}

	@Override
	public Future<TID> outOfOrderInsert(final TIN entry) {
		// Buffer out-of-order insertions in sorted queue
		this.outOfOrderQueue.offer(entry);
		// Mirror queue (unsorted) to disk
		this.outOfOrderQueueDiskMirror.insert(-1, entry);
		// If the queue is full, push it to disk
		if ( this.outOfOrderQueue.size() >= this.outOfOrderQueueSize ) {
			pushOutOfOrderQueue();
		}
		// TODO: Implement Future Handling
		return new InsertFuture();
	}

	/**
	 * Pushes out the out-of-order queue.
	 */
	private void pushOutOfOrderQueue() {
		try {
			this.bufferLock.lock();
			this.buffer.pushOut(); // clears the output buffer before closing
			if ( !this.outOfOrderQueue.isEmpty() ) {
				this.appenderContainer.outOfOrderUpdating = true;
				final IndexEntry rootEntry = createIndexEntry(this.buffer.root());
				this.tree.setRootEntry(rootEntry);
				this.tree.setRootDescriptor((Descriptor) rootEntry.separator().clone());
				final int s = this.outOfOrderQueue.size();
				for (int i = 0; i < s; i++) {
					outOfOrderTreeInsert(this.outOfOrderQueue.poll());
				}
				this.appenderContainer.outOfOrderUpdating = false;
			}
		} finally {
			this.bufferLock.unlock();
		}
		this.outOfOrderQueueDiskMirror.clear();
	}

	/**
	 * Handles out-of-order insertion.
	 *
	 * @param entry
	 *            the entry to be inserted
	 */
	private void outOfOrderTreeInsert(final TIN entry) {
		final long ts = (Long) this.tree.key(entry); // Time stamp of entry
		for (int i = 0; i < this.buffer.treePath.size() - 1; i++) {
			final Node node = this.buffer.get(i);
			final long entryTs = getFirstTimeStamp(node);
			if ( ts >= entryTs ) {// insert into the node
				insertIntoNode(node, entry);
				// Handle full node
				this.appenderContainer.outOfOrderUpdating = false;
				handleFullNode(node, i); // Handle full node
				this.appenderContainer.outOfOrderUpdating = true;
				return;
			} // else succeed with the root
		}
		// if there was nothing to insert into, insert into the root
		final Node node = this.buffer.root();
		this.tree.insert(entry);
		this.appenderContainer.outOfOrderUpdating = false;
		handleFullNode(node, node.level); // Handle full node
		// Update first entry
		this.appenderContainer.outOfOrderUpdating = true;
		if ( getTimestamp(entry) < getTimestamp(this.first) ) {
			this.first = entry;
			// FIXME
			//        informListeners(entry, getEventID(entry)); //TODO: outsource into tree
		}
	}

	/**
	 * Handles pushing out of filled node in case of out-of-order updates.
	 *
	 * @param node
	 *            the node to be checked
	 * @param level
	 *            the level of the node
	 */
	private void handleFullNode(final Node node, final int level) {
		if ( treatFullNode(node, level) ) {
			this.buffer.pushOut(); // push buffer directly
			final IndexEntry rootEntry = createIndexEntry(this.buffer.root());
			this.tree.setRootEntry(rootEntry);
			this.tree.setRootDescriptor((Descriptor) rootEntry.separator().clone());
		}
	}
	
	protected boolean treatFullNode(final Node n, final int level) {
		if ( isFull(n, level) ) {
			if ( this.buffer.isFull() ) { // Push buffer only after handling leaf overflow
				this.buffer.pushOut();
			}
			//FIXME: This seems wrong
			this.buffer.handleFullNode(level);
			this.buffer.getForAdd(level);
			//this.buffer.nextNode(level);
			return true;
		}
		return false;
	}
	

	/**
	 * Inserts the given entry into the given subtree.
	 *
	 * @param node
	 *            the subtree to insert into
	 * @param entry
	 *            the entry to be inserted
	 */
	private void insertIntoNode(final Node node, final Object entry) {
		if ( node.level == LEAF_LEVEL ) {
			node.grow(entry);
		}
		else {
			final Comparable<?> c = this.tree.key(entry);
			final Descriptor descriptor = this.tree.createKeyRange(c, c);
			// Create Stack
			final Stack<MapEntry<?, ?>> path = new Stack<>();
			Tree.IndexEntry ie = createIndexEntry(node);
			do {
				ie = ie.chooseSubtree(descriptor, path);
			} while (ie.level() > LEAF_LEVEL);
			// Insert into the node
			ie.growAndPost(entry, path);
			// Inform listeners
			if ( ((Long) ie.id()).longValue() != this.buffer.treePath.get(0).id ) {
				// FIXME
				//                informListeners(entry, new EventID((Long)ie.id, 0, (Long)tree.key(entry)));
			}
		}
	}

	/**
	 * Creates an index entry pointing to the given node.
	 *
	 * @param node
	 *            the node
	 * @return an index entry pointing to the node
	 */
	private IndexEntry createIndexEntry(final Node node) {
		final IndexEntry ie = this.tree.createIndexEntry(node.level + 1)
				.initialize(this.buffer.treePath.get(node.level).id);
		Separator sep;
		if ( node.number() > 0 ) {
			sep = (Separator) this.tree.separator(node.getLast()).clone();
		}
		else {
			sep = (Separator) this.tree.separator(node.previousNeighbor().get().getLast()).clone();
		}
		ie.initialize(sep);
		initializeIndexEntry(ie, node);
		return ie;
	}

	/**
	 * Returns the first time stamp if the node
	 *
	 * @param node
	 *            the node
	 * @return the first time stamp of the node
	 */
	private long getFirstTimeStamp(final Node node) {

		final Node n = (Node) node;
		if ( n.previousNeighbor() == null )
			return Long.MAX_VALUE;
		else {
			final Node prevNode = (Node) n.previousNeighbor().get();
			if ( prevNode != null ) {
				// Only update if the node is not already in the buffer (to avoid overwriting without flushing!!)
				if ( !this.appenderContainer.outOfOrderBuffer.contains(this.appenderContainer,
						(Long) n.previousNeighbor().id()) ) {
					this.appenderContainer.outOfOrderBuffer.update(this.appenderContainer, (Long) n.previousNeighbor().id(),
							prevNode, null, true); // Add previous neighbors of right flank to buffer
				}
				if ( prevNode.level == LEAF_LEVEL )
					return (Long) this.tree.key(prevNode.getLast());
				else
					return (Long) ((IndexEntry) prevNode.getLast()).separator().sepValue();
			}
			else
				return Long.MAX_VALUE;
		}
	}

	/**
	 * Extracts the timestamp from the given event
	 *
	 * @param event
	 *            the event
	 * @return the events timestamp
	 */
	private long getTimestamp(final Object event) {
		return ((Long) this.tree.key(event)).longValue();
	}

	// ============================================================================================================

	@Override
	public void flushPathAndClose() {
		this.buffer.close(); // Close the appender and write everything to disk
		tree.setContainer(this.appenderContainer.container);
	}

	@Override
	public void flushPath() {
		if ( !this.buffer.treePath.isEmpty() ) {
			getTempRoot(true);
		}
		this.appenderContainer.flush();
	}
	
	public void emptyTreeBuffer() {
		appenderContainer.outOfOrderBuffer.flushAll(appenderContainer);
		appenderContainer.outOfOrderBuffer.removeAll(appenderContainer);
	}

	@Override
	public boolean hasData() {
		return true;
	}

	@Override
	public DoubleCursor<TOUT> query(final Long minKey, final Long maxKey) {
		if ( this.buffer.treePath.isEmpty() ||
			 this.keyFunction.apply(first).compareTo(maxKey) > 0 || 
			 this.keyFunction.apply(last).compareTo(minKey) < 0)
			return new EmptyDoubleCursor<>();
		getTempRoot(true);
		return this.tree.rangeQueryWithIds(generateOut, minKey, maxKey);
	}
	
	public DoubleCursor<TOUT> find(TID id) throws NoSuchEventException {
		return new LeafLevelCursor<>(container(), id, generateOut);
	}

	// ============================================================================================================

	/**
	 * Initializes the given index entry pointing to the given node.
	 *
	 * @param entry
	 *            the new index entry pointing to node
	 * @param node
	 *            the node the index entry points to
	 */
	protected void initializeIndexEntry(final IndexEntry entry, final Node node) {
	}

	/**
	 * Adds the entry to the given node.
	 *
	 * @param n
	 *            the node
	 * @param entry
	 *            the entry to be added to the node
	 */
	protected void updateNode(final Node n, final Object entry) {
		n.entries.add(entry);
	}

	// ============================================================================================================

	/**
	 * Inserts the given entry into the given level.
	 *
	 * @param entry
	 *            the entry to insert
	 * @param level
	 *            the level of the entry
	 */
	protected void insert(final Object entry, final int level) {

		if ( level >= this.treeSize ) {
			resizeTree(level); // Add new level to the tree
		}

		// Start fresh node if necessary
		Node n = this.buffer.getForAdd(level);
		updateNode(n, entry);
		// Write out if full
		if ( isFull(n, level) )
			this.buffer.handleFullNode(level);
	}
	
	/**
	 * Adds the given level to the tree.
	 *
	 * @param level
	 *            the level to be added.
	 */
	private void resizeTree(final int level) {
		// create new level in tree path
		final Node node = (Node) this.tree.createNode(level);
		this.buffer.add(node, level);
		this.treeSize++;
	}

	/**
	 * Creates a temporary root for the tree.
	 */
	@Override
	public Pair<IndexEntry, Node> getTempRoot(final boolean updateAggregates) {
		if ( updateAggregates ) {
			pushOutOfOrderQueue(); // Push out the out-of-order queue
		}
		final Pair<IndexEntry, Node> e = this.buffer.copyRoot();
		this.tree.setRootEntry(e.getElement1());
		this.tree.setRootDescriptor(this.tree.createKeyRange(this.tree.key(this.first), this.tree.key(this.last)));
		return e;
	}

	/**
	 * Checks if the given node is full
	 *
	 * @param n
	 *            the node
	 * @param level
	 *            the level of the node
	 * @return true if the node is full, false otherwise
	 */
	protected boolean isFull(final Node n, final int level) {
		return n.entries.size() >= ((level == LEAF_LEVEL) ? this.maxLeafEntries : this.maxIndexEntries);
	}

	/**
	 * Checks if the given node is full
	 *
	 * @param n
	 *            the node
	 * @param level
	 *            the level of the node
	 * @return true if the node is full, false otherwise
	 */
	private boolean isNearlyFull(final Node n, final int level) {
		return n.entries.size() + 1 == ((level == LEAF_LEVEL) ? this.maxLeafEntries : this.maxIndexEntries);
	}

	/**
	 * @return the first
	 */
	public TIN getFirst() {
		return this.first;
	}

	/**
	 * @return the last
	 */
	public TIN getLast() {
		return this.last;
	}

	// ================================================================================================================
	/**
	 * A container considering the buffer content.
	 */
	private class AppenderContainer extends AbstractContainer {

		/** The container */
		private final Container					container;
		/** The buffer */
		AppenderBuffer							buffer;
		/** The buffer for out-of-order inserts */
		protected Buffer<Object, Long, Object>	outOfOrderBuffer;
		/** Flag indicating if an out-of-order updating is active **/
		protected boolean						outOfOrderUpdating;

		public AppenderContainer(final Container container) {
			this.container = container;
		}

		/**
		 * Sets the buffer for the container.
		 *
		 * @param buffer
		 *            the buffer
		 */
		public void setBuffer(final AppenderBuffer buffer, final Buffer<Object, Long, Object> outOfOrderBuffer) {
			this.buffer = buffer;
			this.outOfOrderBuffer = outOfOrderBuffer;
		}

		@Override
		public void close() {
			super.close();
			this.outOfOrderBuffer.flushAll(this);
			this.container.close();
		}

		@Override
		public synchronized Object get(final Object id, final boolean unfix) throws NoSuchElementException {
			try {
				FastAppender.this.bufferLock.lock();
				final long idVal = (Long) id;
	
				if ( this.buffer.containsEntry(idVal) ) {
					for ( BufferEntry entry : this.buffer.treePath ) {
						if ( entry.id == idVal ) {
							if ( this.outOfOrderUpdating )
								return entry.node;
							else
								return copyNode(entry.node, true);
						}
					}
					for ( BufferWriteEntry entry : this.buffer.nodeBuffer ) {
						if ( entry.id == idVal ) {
							return entry.node;
						}
					}
				}
				final Object result = this.outOfOrderBuffer.get(this, idVal, new AbstractFunction<Long, Object>() {
	
					/** The serialVersionUID */
					private static final long serialVersionUID = 1L;
	
					@Override
					public Object invoke(final Long argument) {
						return AppenderContainer.this.container.get(argument, true);
					}
				}, true);
				return result;
			} finally {
				FastAppender.this.bufferLock.unlock();
			}
		}

		@Override
		public Iterator ids() {
			final SortedSet<Long> inMemIds = new TreeSet<>();
			for ( BufferEntry be : buffer.treePath ) {
				inMemIds.add( be.id );
			}
			for ( BufferWriteEntry bwe : buffer.nodeBuffer ) {
				inMemIds.add( bwe.id );
			}

			return new Iterator () {
				final Iterator containerSource = container.ids();
				Iterator remaining = null;

				/**
				 * @{inheritDoc}
				 */
				@Override
				public boolean hasNext() {
					return containerSource.hasNext() || remaining.hasNext();
				}

				/**
				 * @{inheritDoc}
				 */
				@Override
				public Object next() {
					Object result = null;
					if ( containerSource.hasNext() ) {
						result = containerSource.next();
						inMemIds.remove(result);
						
						if ( !containerSource.hasNext() ) {
							remaining = inMemIds.iterator();
						}
					}
					else if ( remaining.hasNext() ) {
						result = remaining.next();
					}
					else {
						throw new NoSuchElementException("Cannot go beyond last element.");
					}
					return result;
				}
				
				
				
			};
		}

		@Override
		public boolean isUsed(final Object id) {
			final long idVal = (Long) id;
			if ( this.buffer.containsEntry(idVal) )
				return true;
			else
				return this.container.isUsed(id);
		}

		@Override
		public FixedSizeConverter<?> objectIdConverter() {
			return this.container.objectIdConverter();
		}

		@Override
		public void remove(final Object id) throws NoSuchElementException {
			this.container.remove(id);
		}

		@SuppressWarnings({ "rawtypes" })
		@Override
		public Object reserve(final Function getObject) {
			Object oj = this.container.reserve(getObject);
			// Handles out-of-order inserts
			while (getObject != null && this.buffer.containsEntry((Long) oj)) {
				oj = this.container.reserve(getObject);
			}
			return oj;
		}

		@Override
		public int size() {
			return this.container.size();
		}

		@Override
		public void update(final Object id, final Object object, final boolean unfix) throws NoSuchElementException {
			
			try {
				FastAppender.this.bufferLock.lock();
				if ( !unfix && this.buffer.containsEntry((Long) id) ) { // out-of-order update from the tree
					this.buffer.replace((Long) id, (Node) object);
				}
				else {
					final long lid = (Long) id;
					if ( lid > this.buffer.treePath.get(0).id ) {
						if ( ((Node) object).level >= this.buffer.treePath.size() ) {
							logger.debug("New root node");
							this.buffer.treePath.add(new BufferEntry((Long) id, (Node) object));
							return;
						}
						this.container.update(id, object, unfix);
					}
					if ( this.outOfOrderUpdating ) {
						this.outOfOrderBuffer.update(this, lid, object, new AbstractFunction<Object, Object>() {
	
							/** The serialVersionUID */
							private static final long serialVersionUID = 1L;
	
							@Override
							public Object invoke(final Object argument0, final Object argument1) {
								AppenderContainer.this.container.update(argument0, argument1, unfix);
								return null;
							}
						}, true);
						return; // Updates are propagated by the buffer
					}
					else {
						this.outOfOrderBuffer.update(this, lid, object, null, true);
						if ( lid <= this.buffer.treePath.get(0).id ) {
							this.container.update(id, object, unfix);
						}
					}
				}
			} finally {
				FastAppender.this.bufferLock.unlock();
			}
		}

		@Override
		public void flush() {
			super.flush();
			this.outOfOrderBuffer.flushAll(this);
			this.container.flush();
		}

		@Override
		public int hashCode() {
			return 0;
		}
	}

	// ================================================================================================================

//	/**
//	 * Returns the fill rate of the given node.
//	 *
//	 * @param n
//	 *            the node
//	 * @return the nodes fill rate
//	 */
//	private double getFillRate(final Node n) {
//		final double d = n.number();
//		final double b = n.level == LEAF_LEVEL ? this.tree.getLeafNodeB() : this.tree.getIndexNodeB();
//		return d / b;
//	}

	/**
	 * Copies the given node and inserts the current tree path state
	 *
	 * @param node
	 *            the node to be copied
	 * @return a copy of the node with the latest state from the tree buffer
	 */
	protected Node copyNode(final Node node, final boolean updateAggregates) {
		final Node copy = (Node) this.tree.createNode(node.level);
		copy.setNextNeighbor(node.nextNeighbor());
		copy.setPreviousNeighbor(node.previousNeighbor());
		copy.entries.addAll(node.entries);
		return copy;
	}

	// ================================================================================================================

	/**
	 * Update the nodes references.
	 *
	 * @param oldPair
	 *            the previous node
	 * @param newPair
	 *            the next node
	 * @param level
	 *            the level
	 */
	protected void updateReferences(final BufferEntry oldPair, final BufferEntry newPair, final int level) {
		oldPair.node.setNextNeighbor(this.tree.createIndexEntry(level).initialize(newPair.id));
		newPair.node.setPreviousNeighbor(this.tree.createIndexEntry(level).initialize(oldPair.id));
	}

	// ================================================================================================================

//	/**
//	 * Logs the state of this appender
//	 */
//	private String logState() {
//		final StringBuilder sb = new StringBuilder();
//		sb.append("=============================");
//		sb.append("\r\n");
//		// Variables
//		sb.append("Variables");
//		sb.append("\r\n");
//		sb.append("-----------------------------");
//		sb.append("\r\n");
//		sb.append("first: " + eventToString(this.first));
//		sb.append("\r\n");
//		sb.append("last: " + eventToString(this.last));
//		sb.append("\r\n");
//		sb.append("treeSize: " + this.treeSize);
//		sb.append("\r\n");
//		sb.append("hasData: " + this.hasData);
//		sb.append("\r\n");
//		sb.append("-----------------------------");
//		sb.append("\r\n");
//		// tree path
//		sb.append("Tree path");
//		sb.append("\r\n");
//		sb.append("-----------------------------");
//		sb.append("\r\n");
//		for (int i = 0; i < this.buffer.treePath.size(); i++) {
//			final BufferEntry be = this.buffer.treePath.get(i);
//			sb.append("[" + i + "]: " + be.id + " - " + be.node.number() + " --> " + eventToString(be.node.getLast()));
//			sb.append("\r\n");
//		}
//		sb.append("Node buffer size: " + this.buffer.nodeBuffer.size());
//		sb.append("\r\n");
//		if ( this.buffer.nodeBuffer.size() > 0 ) {
//			final BufferEntry be = this.buffer.nodeBuffer.get(0);
//			sb.append(be.id + " - " + be.node.number() + " --> " + eventToString(be.node.getLast()));
//			sb.append("\r\n");
//		}
//		sb.append("=============================");
//		return sb.toString();
//	}
//
//	/**
//	 * Converts an event to string.
//	 *
//	 * @param n
//	 *            the event
//	 * @return the event`s string representation
//	 */
//	private String eventToString(final Object n) {
//		if ( n == null )
//			return "null";
//		else if ( n.getClass().isArray() )
//			return Arrays.toString((Object[]) n);
//		else
//			return n.toString();
//	}

	// ================================================================================================================

	/**
	 * Informs all push listeners about the push-out.
	 *
	 * @param entry
	 *            the buffer entry
	 */
	private void informListeners(final BufferEntry entry) {
		if ( entry.node.level > LEAF_LEVEL )
			return;
		// FIXME
		for (int i = 0; i < entry.node.number(); i++) {
			//            for (EventPushListener epl : eventPushListeners)
			//                epl.insertEvent(
			//                        entry.node.getEntry(i),
			new TID(entry.id, i);//);
		}
	}

	// ================================================================================================================

	protected class AppenderBuffer {

		/** The id step size of the underlying container. */
		private final int				idStepSize;
		/** The size of the buffer */
		private final int				bufferSize;
		/** The output buffer */
		private final List<BufferWriteEntry>	nodeBuffer;
		/** The buffer for the tree`s right flank */
		protected List<BufferEntry>		treePath;
		/** The underlying container */
		protected AppenderContainer		container;

		/**
		 * Creates a new Buffer.
		 *
		 * @param container
		 *            the underlying container
		 * @param bufferSize
		 *            the size of the output buffer
		 * @param idStepSize
		 *            the id step size of the container
		 */
		public AppenderBuffer(final AppenderContainer container, final int bufferSize, final int idStepSize) {
			this.container = container;
			this.bufferSize = bufferSize;
			this.idStepSize = idStepSize;
			this.nodeBuffer = new ArrayList<>(bufferSize);
			this.treePath = new ArrayList<>();
		}

		/**
		 * Returns the path node at the given level.
		 *
		 * @param level
		 *            the level of the node
		 * @return the current node from the tree path
		 */
		public Node get(final int level) {
			return this.treePath.get(level).node;
		}

		/**
		 * Returns the root of the right flank.
		 *
		 * @return the root of the right flank
		 */
		public Node root() {
			return this.treePath.get(this.treePath.size() - 1).node;
		}

		/**
		 * Adds the given node to the tree path.
		 *
		 * @param n
		 *            the node
		 * @param level
		 *            the level of the node
		 */
		public void add(final Node n, final int level) {
			// The id of the node
			final long nextLevelID = firstLevelID(level);
			this.treePath.add(level, new BufferEntry(nextLevelID, n));
		}

		/**
		 * Returns if this buffer contains the given id.
		 *
		 * @param id
		 *            the id of the block
		 * @return true, if the buffer contains this entry, false otherwise
		 */
		public boolean containsEntry(final long id) {
			for (final BufferEntry be : this.treePath)
				if ( be.id == id )
					return true;
			for (final BufferWriteEntry be : this.nodeBuffer)
				if ( be.id == id )
					return true;
			return false;
		}

		/**
		 * Replaces the node with the given id.
		 *
		 * @param id
		 *            the node`s id
		 * @param node
		 *            the node
		 */
		public void replace(final long id, final Node node) {
			for (final BufferEntry be : this.treePath) {
				if ( be.id == id ) {
					if ( node.level() != be.node.level() ) 
						throw new RuntimeException("Wrong node [" + id + "] at level " + node.level());
					be.node = copyNode(node,false);
					break;
				}
			}
			for (final BufferWriteEntry be : this.nodeBuffer) {
				if ( be.id == id ) {
					if ( node.level() != be.node.level() ) 
						throw new RuntimeException("Wrong node [" + id + "] at level " + node.level());
					be.node = copyNode(node, false);
					break;
				}
			}
		}

		/**
		 * Returns if this buffer is full.
		 *
		 * @return true if this buffer is full, false otherwise
		 */
		public boolean isFull() {
			return this.nodeBuffer.size() >= this.bufferSize;
		}

		/**
		 * Pushes the output buffer to disk.
		 */
		public void pushOut() {
			try {
				FastAppender.this.bufferLock.lock();
				for ( BufferWriteEntry entry : this.nodeBuffer ) {
					// Add forward linking
					if ( entry.nextId != null ) {
						IndexEntry ie = tree.createIndexEntry(entry.node.level+1).initialize(entry.nextId);
						entry.node.setNextNeighbor(ie);
					}
					this.container.update(entry.id, entry.node, true);
					informListeners(entry); // Inform listeners
				}
				this.nodeBuffer.clear();
			} finally {
				FastAppender.this.bufferLock.unlock();
			}
		}

		/**
		 * Calculates the number of nodes in the upper levels to determine the
		 * total offset for the next node. Therefore, if a node is nearly full
		 * (except for the last entry), the next entry will cause an overflow.
		 * This results in an additional node between two nodes of the given
		 * level and therefore has to be considered in the calculation of the
		 * next node`s address.
		 *
		 * @param level
		 *            the level for which additional offset on upper levels
		 *            should be calculated
		 * @return the number of upper level nodes between the current and the
		 *         next node of the given level
		 */
		private int getAdditionalOffset(final int level) {
//			if ( level < this.treePath.size() && FastAppender.this.isFull(this.treePath.get(level).node, level) )
			if ( level < this.treePath.size() && isNearlyFull(this.treePath.get(level).node, level) )
				return this.idStepSize + getAdditionalOffset(level + 1);
			else
				return 0;
		}
		

		/**
		 * Full nodes were already written out
		 * @param level
		 * @return
		 */
		public Node getForAdd(final int level) {
			final BufferEntry be = this.treePath.get(level);
			if ( FastAppender.this.isFull(be.node,level) ) {
				final Node newNode = FastAppender.this.tree.createNode(level);
//				final long newID   = nextLevelID(be.id, level);
				
				
				if ( be.nextId == null ) 
					throw new IllegalStateException("Full nodes must have a set nextId (predicted next neighbor)");
				
				final BufferEntry newPair = new BufferEntry(be.nextId, newNode);
				
				updateReferences(be, newPair, level);
				
				try {
					FastAppender.this.bufferLock.lock();
					this.treePath.set(level, newPair);
				} finally {
					FastAppender.this.bufferLock.unlock();
				}
				return newNode;
			}
			else {
				return be.node;
			}
		}
		
		public void handleFullNode(final int level) {
			final BufferEntry be = this.treePath.get(level);
			final long        realId   = (Long) this.container.reserve(null);
			
			// There have been out-of-order updates in the mean-time
			if ( realId != be.id ) {
				logger.warn("Repairing ID. Level=" + level + ", predicted=" + be.id + ", actual=" + realId);
				System.out.println("Repairing ID. Level=" + level + ", predicted=" + be.id + ", actual=" + realId);
				repairReferences((Node) be.node, be.id, realId);
				be.id = realId;
			}
			
			// Create index entry for upper level
			final Separator sep = (Separator) FastAppender.this.tree.separator(be.node.getLast()).clone();
			final IndexEntry entry = FastAppender.this.tree.createIndexEntry(level + 1).initialize(be.id,sep);

			// Updates the aggregates
			initializeIndexEntry(entry, be.node);
			
			// Write out
			try {
				FastAppender.this.bufferLock.lock();
				// Generate next id and add page to write buffer
				be.nextId = nextLevelID(be.id, level);
				this.nodeBuffer.add(new BufferWriteEntry(be.id, copyNode(be.node,false), be.nextId));
			} finally {
				FastAppender.this.bufferLock.unlock();
			}
			
			if ( isFull() ) {
				pushOut();
			}
			
			
			// Insert stuff into upper levels
			insert(entry, level+1);
		}

		/**
		 * Repairs the references to and from the nodes.
		 *
		 * @param n
		 *            the node
		 * @param id
		 *            the estimated id of the node
		 * @param newID
		 *            the new (real) is of the node
		 */
		private void repairReferences(final Node n, final Comparable id, final long newID) {
			IndexEntry ie = n.previousNeighbor();
			if ( ie == null ) // The root
				return;
			
			// We need to check, if previous neighbor is still in the node buffer
			
			
			Node prev = (Node) ie.get();

			while (!prev.nextNeighbor().id().equals(id)) {
				ie = prev.nextNeighbor();
				prev = (Node) ie.get();
			}
			n.previousNeighbor().initialize(ie.id());
			prev.nextNeighbor().initialize(newID);
			ie.update(prev);
		}
		
		private Optional<BufferWriteEntry> getFromNodeBuffer( long id ) {
			for ( BufferWriteEntry be : nodeBuffer ) {
				if ( be.id == id )
					return Optional.of(be);
			}
			return Optional.empty();
		}

		/**
		 * Returns the next id for the given node
		 *
		 * @param id
		 * @param level
		 * @return
		 */
		protected long nextLevelID(final long id, final int level) {
			return id + idsBetween(level) * this.idStepSize + this.idStepSize + getAdditionalOffset(level + 1);
		}

		/**
		 * Returns the first ID for the given level.
		 *
		 * @param level
		 *            the level
		 * @return the first id for the given level
		 */
		protected long firstLevelID(final int level) {
			return idsBetween(level) * this.idStepSize;
		}

		/**
		 * Calculates the number of ids (blocks) between two nodes of the given
		 * level. This is useful to determine the ID of the next neighbor.
		 *
		 * @param level
		 *            the level of the node
		 * @return the number of nodes in lower levels between two nodes of the
		 *         given level
		 */
		private long idsBetween(final int level) {
			long output = 0;
			for (int i = 1; i <= level; i++) {
				output += Math.pow(FastAppender.this.maxIndexEntries, i);
			}
			return output;
		}

		/**
		 * Creates an index entry for the given node.
		 *
		 * @param node
		 *            the node to create an index entry for
		 * @param id
		 *            the node id
		 * @return an index entry pointing to the given node
		 */
		protected IndexEntry createIndexEntry(final Node node, final long id) {
			final Separator sep = (Separator) FastAppender.this.tree.separator(node.getLast()).clone();
			final IndexEntry newIndexEntry = FastAppender.this.tree.createIndexEntry(node.level + 1).initialize(id,sep);
			// further initialization of the new entry
			initializeIndexEntry(newIndexEntry, node);
			return newIndexEntry;
		}

		/**
		 * Creates a root entry with a deep copy of the tree path.
		 *
		 * @return the new root entry
		 */
		protected Pair<IndexEntry, Node> copyRoot() {

			IndexEntry child = null;
			Node node = null;
			if ( this.treePath.get(0).node.number() > 0 ) {
				node = this.treePath.get(0).node;
				child = createTmpIndexEntry(node, this.treePath.get(0).id);
				//                child = createIndexEntry(node, treePath.get(0).id);
			}
			for (int i = 1; i < this.treePath.size(); i++) {
				final BufferEntry e = this.treePath.get(i);
				node = copyNode(e.node, true);
				if ( child != null && (node.number() == 0 || !((IndexEntry)node.getLast()).id().equals(child.id())) ) {
					node.entries.add(child);
				}
				if ( node.number() > 0 ) {
					child = createTmpIndexEntry(node, e.id);
					//                    child = createIndexEntry(node, e.id);
				}
				else {
					child = null;
				}
			}
			return new Pair<>(child, node);
		}

		protected IndexEntry createTmpIndexEntry(final Node node, final Object id) {
			final Separator sep = (Separator) FastAppender.this.tree.separator(node.getLast()).clone();
			final IndexEntry newIndexEntry = FastAppender.this.tree.createTempIndexEntry(node);
			newIndexEntry.initialize(sep).initialize(id);
			// further initialization of the new entry
			initializeIndexEntry(newIndexEntry, node);
			return newIndexEntry;
		}

		/**
		 * Returns the root entry for the node.
		 *
		 * @return the trees root entry
		 */
		protected void close() {
			
			// No data.
			if ( FastAppender.this.last == null ) {
				FastAppender.this.tree.setRootEntry(null);
				FastAppender.this.tree.setRootDescriptor(null);
				logger.debug("Closed FastAppender (no-data).");
				return;
			}
			
			
			logger.debug("Closing FastAppender.");
			// push buffer to disk
			logger.debug("Pushing out-of-order queue.");
			pushOutOfOrderQueue();
			
			
			// Separator from last inserted event
			final Separator sep = (Separator) FastAppender.this.tree.separator(FastAppender.this.last).clone();
			// Link references up the tree
			
			logger.debug("Persisting right tree flank.");
			
			
			for ( int i = 0; i < this.treePath.size(); i++ ) {
				BufferEntry be = this.treePath.get(i);
				final Node node = be.node;
				long id = be.id;
				
				// Page was flushed -- Nothing to do.
				if ( be.nextId != null ) {
					logger.debug("Level " + node.level + " page was already persisted.");
				}
				else {
					id = (Long) this.container.reserve(null);
					final IndexEntry nn = FastAppender.this.tree.createIndexEntry(node.level + 1).initialize(-be.id);
					logger.debug("Level " + node.level + " is persisted to " + id + ", predicted id (" + be.id + ") is written to nextNeighbor reference.");
					node.setNextNeighbor(nn);
					
					// Non root node
					if ( node.level() < treePath.size()-1 ) {
						final IndexEntry parentIndexEntry = FastAppender.this.tree.createIndexEntry(node.level + 1).initialize(id,sep);
						initializeIndexEntry(parentIndexEntry, node);
						insert(parentIndexEntry, node.level() + 1);
					}
					this.container.container.update(id, node, true);
				}
				
				// Root node handling - Set tree root
				if ( node.level() == treePath.size()-1 ) {
					final IndexEntry parentIndexEntry = FastAppender.this.tree.createIndexEntry(node.level + 1).initialize(id,sep);
					FastAppender.this.tree.setRootEntry(parentIndexEntry);
					FastAppender.this.tree.setRootDescriptor(FastAppender.this.tree.createKeyRange(
							FastAppender.this.tree.key(FastAppender.this.first),
							FastAppender.this.tree.key(FastAppender.this.last)));
				}
			}
			// ensure flushing of all buffers!
			pushOut();
			logger.debug("Flushing random access buffer.");
			FastAppender.this.appenderContainer.flush();
			logger.debug("Finished closing FastAppender.");
		}

		// DEBUGGING PURPOSES
		//        protected void logTree() {
		//			for (int i = treePath.size() - 1; i >= 0; i--) {
		//				System.out.println("Level " + i);
		//				Long id = treePath.get(i).id;
		//				Node n = (Node) treePath.get(i).node;
		//
		//				do {
		//					Long prevId = n.previousNeighbor != null ? (Long) n.previousNeighbor.id() : null;
		//					Long nextId = n.nextNeighbor != null ? (Long) n.nextNeighbor.id() : null;
		//					if ( i > 0 ) {
		//						System.out.print(String.format("  [%s <-- %d --> %s]", prevId, id, nextId));
		//						System.out.print( ", Entries: ");
		//						for ( Object o : n.entries ) {
		//							System.out.print(o);
		//							System.out.print(", ");
		//						}
		//						System.out.println();
		//					}
		//					else
		//						System.out.println(String.format("  [%s <-- %d --> %s]", prevId, id, nextId));
		//
		//					id = prevId;
		//					if ( prevId != null )
		//						n = (Node) n.previousNeighbor.get();
		//				} while (id != null);
		//			}
		//        }

		// ============================================================================================================

		/**
		 * Loads the data from a successfully written tree
		 */
		protected void load() {
			// Load complete right flank of BPlusTree into treePath
			IndexEntry ie = FastAppender.this.tree.rootEntry();
			FastAppender.this.treeSize = ie.level() + 1;
			Node node = (Node) ie.get();
			long realID;
			long id = (Long) ie.id();

			List<Long> idsToDelete = new ArrayList<>();
			
			// When closing, the entries are stored in the next
			// container position, but the estimated id is not touched
			// Therefore, a nextNeighbour reference is created with the
			// physical id of the entry. This is handled here!
			if ( node.level > LEAF_LEVEL ) { // Root is leaf
				do {
					// Negative values indicate a proper shutdown
					if ( node.nextNeighbor() != null && ((Long)node.nextNeighbor().id()) <= 0L ) {
						realID = -((Long) node.nextNeighbor().id());
						node.setNextNeighbor(null);
						idsToDelete.add(id);
						treePath.add(0,new BufferEntry(realID, node));
					}
					// Node was full and already written
					else if ( node.nextNeighbor() != null ) {
						realID = id;
						long nextId = (Long) node.nextNeighbor().id();
						node.setNextNeighbor(null);
						treePath.add(0,new BufferEntry(realID, node, nextId));
					}
					
					else {
						throw new IllegalStateException("Right flank must posses a nextNeighbour with negative id value.");
					}
					
					// Switch to child
					if ( node.level > LEAF_LEVEL ) {
						ie = (IndexEntry) node.getLast();
						id = (Long) ie.id();
						
						Node childNode = (Node) ie.get();
						
						if ( !FastAppender.this.isFull(childNode, childNode.level) )
							node.entries.remove(node.entries.size() - 1); // Remove last entry from the node
						
						node = childNode;
					}

				} while (node.level > LEAF_LEVEL);
			}
			
			
			// Add leaf id.
			if ( node.nextNeighbor() != null && ((Long)node.nextNeighbor().id()) <= 0 ) {
				realID = -((Long) node.nextNeighbor().id());
				node.setNextNeighbor(null);
				idsToDelete.add(id);
				treePath.add(0,new BufferEntry(realID, node));
			}
			// Leaf was full
			else if ( node.nextNeighbor() != null ) {
				realID = id;
				long nextId = (Long) node.nextNeighbor().id();
				node.setNextNeighbor(null);
				treePath.add(0,new BufferEntry(realID, node, nextId));
			}
			else {
				throw new IllegalStateException("Right flank must posses a nextNeighbour with negative id value.");
			}
			
			// Remove persisted right flank
			logger.debug("Deleting flank pages: " + idsToDelete);
			Collections.sort(idsToDelete, Comparator.comparingLong(x -> (Long)x).reversed() );
			idsToDelete.forEach(x -> this.container.remove(x));


			// Set real id to root
			FastAppender.this.tree.rootEntry().initialize(this.treePath.get(this.treePath.size() - 1).id);

			// Find first and last event
			Node lastLeaf = treePath.get(LEAF_LEVEL).node;
			
			while (lastLeaf.number() < 1 && lastLeaf.previousNeighbor() != null ) {
				lastLeaf = lastLeaf.previousNeighbor().get();
			}
			if ( lastLeaf.number() > 0 ) {
				FastAppender.this.last = (TIN) lastLeaf.getLast();
			}
			else {
				return;
			}
			
			// Try to get first inserted entry
			if ( FastAppender.this.last != null ) {
				Node n = null;
				for (int level = treePath.size()-1; n == null && level >= 0; level-- ) {
					n = treePath.get(level).node;
					if ( n.number() == 0 )
						n = null;
				}
				while ( n.level() > LEAF_LEVEL ) {
					n = ((IndexEntry) n.getFirst()).get();
				}
				FastAppender.this.first = (TIN) n.getFirst();
			}
			else {
				logger.error("No entry found while loading appender state!");
			}
		}
	}

	
	/**
	 * @return the maxLeafEntries
	 */
	@Override
	public int getMaxLeafEntries() {
		return this.maxLeafEntries;
	}

	
	/**
	 * @return the maxIndexEntries
	 */
	@Override
	public int getMaxIndexEntries() {
		return this.maxIndexEntries;
	}
	
	
}
