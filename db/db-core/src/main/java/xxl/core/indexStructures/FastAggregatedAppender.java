package xxl.core.indexStructures;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Stack;

import sigmod2021.db.core.primaryindex.queries.range.AttributeRange;
import sigmod2021.db.core.wal.Log;
import sigmod2021.db.event.Persistent;
import sigmod2021.db.event.TID;
import xxl.core.collections.MapEntry;
import xxl.core.cursors.AbstractCursor;
import xxl.core.cursors.Cursor;
import xxl.core.functions.Functional.BinaryFunction;
import xxl.core.indexStructures.FastAggregatedBPlusTree.IndexEntry;
import xxl.core.indexStructures.FastAggregatedBPlusTree.Node;
import xxl.core.util.Pair;

/**
 * Appender for fast aggregation format.
 */
public class FastAggregatedAppender<T,TOUT extends Persistent<T>> extends FastAppender<T,TOUT> {

	/**
	 * Creates a new appender for the given tree. Therefore, the appender
	 * replaces the tree`s container.
	 *
	 * @param tree
	 *            the tree
	 * @param bufferSize
	 *            the size of the output buffer
	 */
	public FastAggregatedAppender(final FastAggregatedBPlusTree tree, final int bufferSize, final Log<T> log,
			final Log<T> outOfOrderQueueDiskMirror, final float spare, final int outOfOrderQueueSize,
			final int treeBufferSize, final java.util.function.Function<T, Long> getKeyFunction, BinaryFunction<TID,T,TOUT> generateOut) {
		super(tree, bufferSize, 1, log, outOfOrderQueueDiskMirror, spare, outOfOrderQueueSize, treeBufferSize,
				getKeyFunction, generateOut);
	}

	/**
	 * Creates an appender for recovery.
	 *
	 * @param tree
	 *            the tree
	 * @param maxLeafBufferNodes
	 *            the output buffer size
	 * @param treePath
	 *            the right flank of the tree
	 * @param first
	 *            the first entry
	 * @param last
	 *            the last entry
	 */
	public FastAggregatedAppender(final FastAggregatedBPlusTree tree, final float spare, final int maxLeafBufferNodes, BinaryFunction<TID,T,TOUT> generateOut,
			final List<Pair<Long, ? extends BPlusLink.Node>> treePath, final T first, final T last) {
		super(tree, maxLeafBufferNodes, 1, spare, generateOut, treePath, first, last);
	}
	
	private FastAggregatedBPlusTree tree() {
		return (FastAggregatedBPlusTree) tree;
	}
	

	@Override
	protected void updateNode(final BPlusLink.Node n, final Object entry) {
		super.updateNode(n, entry);
		if ( n.level > LEAF_LEVEL ) {
			final FastAggregatedBPlusTree.Node node = (FastAggregatedBPlusTree.Node) this.buffer.treePath
					.get(n.level - 1).node;
			((FastAggregatedBPlusTree.Node) n).updateAggregates(node);
		}
		else {
			((FastAggregatedBPlusTree.Node) n).updateAggregates(entry);
		}
	}

	@Override
	protected void initializeIndexEntry(final BPlusLink.IndexEntry entry, final BPlusLink.Node node) {
		super.initializeIndexEntry(entry, node);
		((FastAggregatedBPlusTree.Node) node).recalcAggregates();
		((FastAggregatedBPlusTree.IndexEntry) entry).initialize((FastAggregatedBPlusTree.Node) node);
	}

	@Override
	protected Node copyNode(final BPlusLink.Node node, final boolean updateAggregates) {
		final Node n   = (FastAggregatedBPlusTree.Node) super.copyNode(node, updateAggregates);
		final Node old = (FastAggregatedBPlusTree.Node) node;
		n.updateAggregates(old);
		if ( updateAggregates ) {
			for (int i = 0; i < node.level; i++) {
				((FastAggregatedBPlusTree.Node) this.buffer.treePath.get(i).node).recalcAggregates();
				n.updateAggregates((FastAggregatedBPlusTree.Node) this.buffer.treePath.get(i).node);
			}
		}
		return n;
	}

	@Override
	public void recover() {

		// Restore next id for full nodes
		for ( BufferEntry be : this.buffer.treePath ) {
			((Node)be.node).recalcAggregates();
			if ( isFull(be.node, be.node.level) ) {
				long nextId = be.node.nextNeighbor() != null ? (Long) be.node.nextNeighbor().id() : this.buffer.nextLevelID(be.id, be.node.level());
				be.nextId = nextId;
			}
		}
		
		for (int i = 0; i < this.buffer.treePath.size(); i++) {
			recoverLevel(i);
		}
	}

	private void recoverLevel(final int level) {
		final List<BufferEntry> tp = this.buffer.treePath;

		if ( level > tp.size() - 1 )
			return;

		final Long lastId = getLastId(level + 1);

		final BufferEntry top = tp.get(level);
		// Omit current entry
		Node node = (Node) top.node;
		Long id = top.id;

		final List<IndexEntry> indexEntries = new ArrayList<>();

		// This was a non-full node with a previous neighbour - start at predecessor
		if ( !isFull(node, level) && node.previousNeighbor() != null ) {
			id = (Long) node.previousNeighbor().id();
			node = node.previousNeighbor().get();
		}
		// Single incomplete node on level. Not appearing in parent levels.
		else if ( !isFull(node, level) ) {
			id = null;
			node = null;
		}
		// This node is full and was already anchored in parent level.
		else if ( isFull(node, level)  ) {
			// Nothing to do
		}

		// Create all necessary index entries in parent level.
		while (node != null && id > lastId) {
			final Separator sep = (Separator) tree().separator(node.getLast()).clone();
			final IndexEntry entry = tree().createIndexEntry(node.level + 1);
			entry.initialize(sep).initialize(tree().container(), id);
			initializeIndexEntry(entry, node);

			indexEntries.add(entry);

			if ( node.previousNeighbor() != null ) {
				id = (Long) node.previousNeighbor().id();
				node = node.previousNeighbor().get();
			}
			else {
				node = null;
			}
		}

		Collections.reverse(indexEntries);
		for (int i = 0; i < indexEntries.size(); i++) {
			insert(indexEntries.get(i), top.node.level + 1);
		}
	}

	/**
	 * Retrieve the last reachable child-id of the given level
	 *
	 * @param level
	 *            the level to lookup
	 * @return the id of the child-node referenced by the right most index-entry
	 *         of this level
	 */
	private Long getLastId(final int level) {
		if ( level == LEAF_LEVEL )
			throw new IllegalArgumentException("Cannot get LastID on leaf level.");
		if ( level >= this.buffer.treePath.size() )
			return -1L;
		else {
			final BPlusTree.Node n = this.buffer.treePath.get(level).node;
			final BPlusTree.IndexEntry e = (BPlusTree.IndexEntry) n.entries.get(n.entries.size() - 1);
			return (Long) e.id();
		}
	}

	/**
	 * Secondary query on the tree. The resulting cursor returns all(!) items
	 * from a node fulfilling the secondary constraints.
	 *
	 * @param queryRegion
	 * @param attributeRegions
	 * @return
	 */
	public Cursor<TOUT> queryAttributes(final BPlusTree.KeyRange queryRegion,
			final Map<String, AttributeRange<? extends Number>> attributeRegions) {
		return new SecondaryQueryCursor(queryRegion, attributeRegions);
	}
	
	/**
	 * Query cursor for queries on secondary attributes. This cursors returns
	 * all(!) items from a node fulfilling the secondary constraints.
	 */
	// TODO: Improve time bound checking!
	protected class SecondaryQueryCursor extends AbstractCursor<TOUT> {

		private FastAggregatedBPlusTree.Node															cursor;
		private int																						index;
		private boolean																					finished;
		private final Stack<MapEntry<FastAggregatedBPlusTree.IndexEntry, FastAggregatedBPlusTree.Node>>	path		= new Stack<>();
		private final Stack<Integer>																	positions	= new Stack<>();
		private final BPlusTree.KeyRange																queryRegion;
		private final Map<String, AttributeRange<? extends Number>>								        attributeRegions;

		/**
		 * Creates a new secondary query cursor for the given key an attribute
		 * ranges.
		 *
		 * @param queryRegion
		 * @param attributeRegions
		 */
		public SecondaryQueryCursor(final BPlusTree.KeyRange queryRegion,
				final Map<String, AttributeRange<? extends Number>> attributeRegions) {
			this.queryRegion = queryRegion;
			this.attributeRegions = attributeRegions;

			final Pair<BPlusLink.IndexEntry, BPlusLink.Node> root = getTempRoot(true);
			this.cursor = (Node) root.getSecond();
			final IndexEntry ie = (IndexEntry) root.getFirst();
			this.path.push(new MapEntry<>(ie, this.cursor));
		}

		@Override
		public boolean hasNextObject() {
			if ( this.finished )
				return false;

			// Search for a leaf node
			while (this.cursor.level != LEAF_LEVEL || this.index == this.cursor.number()) {
				// If the end of the node is reached => walk up the tree
				while (!this.finished && this.index == this.cursor.number()) {
					if ( this.path.size() > 1 ) {
						treeUp();
					}
					else {
						this.finished = true;
					}
				}

				if ( this.finished || this.cursor.level == LEAF_LEVEL ) {
					this.cursor = null;
					break;
				}
				final IndexEntry entry = (FastAggregatedBPlusTree.IndexEntry) this.cursor.getEntry(this.index++);

				if ( entry.separator().sepValue().compareTo(this.queryRegion.maxBound()) > 0 && (this.index - 1) != 0
						&& ((FastAggregatedBPlusTree.IndexEntry) this.cursor
								.getEntry(this.index - 2)).separator().sepValue()
										.compareTo(this.queryRegion.maxBound()) > 0 ) {     //TODO!!
					// The current entry is beyond the query region => no results expected
					this.finished = true;
					return false;
				}
				else if ( entry.separator().sepValue().compareTo(this.queryRegion.minBound()) >= 0
						&& matchesAttributes(entry) ) {
					// The current entry matches all requirements => go down
					treeDown(entry);
				}
			}

			// Check null case
			if ( this.cursor == null ) {
				this.finished = true;
				return false;
			}
			else
				return true;

		}

		/**
		 * Checks if the given index entry is within the query attribute range
		 *
		 * @param entry
		 *            the entry to check
		 * @return
		 */
		protected boolean matchesAttributes(final FastAggregatedBPlusTree.IndexEntry entry) {
			for (final String s : this.attributeRegions.keySet()) {
				final AttributeRange<? extends Number> targetRange = this.attributeRegions.get(s);
				final Pair<? extends Number, ? extends Number> nodeRange = entry.getMinMax(s);
				
				boolean li = targetRange.isLowerInclusive();
				boolean ui = targetRange.isUpperInclusive();
				
				double tl = targetRange.getLower().doubleValue();
				double tu = targetRange.getUpper().doubleValue();
				
				double nl = nodeRange.getElement1().doubleValue();
				double nu = nodeRange.getElement2().doubleValue();
				
				if ( nodeRange == null ||
					((ui && nl > tu) || (!ui && nl >= tu))||
					((li && nu < tl) || (!li && nu <= tl)) )
					return false;
			}
			return true;
		}

		/** Iterates one level up */
		private void treeUp() {
			if ( this.path.size() > 1 ) {
				final MapEntry<FastAggregatedBPlusTree.IndexEntry, FastAggregatedBPlusTree.Node> entry = this.path
						.pop();
				entry.getKey().unfix(); // Unfix from buffer
				this.cursor = this.path.peek().getValue();
				this.index = this.positions.pop();
			}
			else {
				this.finished = true;
				this.path.clear();
				this.positions.clear();
				this.cursor = null;
			}
		}

		/** Iterates one level down */
		private void treeDown(final IndexEntry entry) {
			this.positions.push(this.index);
			this.index = 0;
			this.cursor = entry.get(false);
			this.path.push(new MapEntry<>(entry, this.cursor));
		}

		@SuppressWarnings("unchecked")
		@Override
		protected TOUT nextObject() {
			if ( this.cursor != null ) {
				TID id = new TID((Long)path.peek().getKey().id(), index++);
				final T result = (T) this.cursor.getEntry(id.getOffset());
				return generateOut.invoke(id, result);
			}
			else
				return null;
		}
	}
}
