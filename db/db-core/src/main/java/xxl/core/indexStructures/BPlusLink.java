package xxl.core.indexStructures;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Iterator;
import java.util.List;

import sigmod2021.db.event.Persistent;
import sigmod2021.db.event.TID;
import xxl.core.collections.containers.Container;
import xxl.core.cursor.DoubleCursor;
import xxl.core.cursor.DoubleCursors;
import xxl.core.functions.Functional.BinaryFunction;
import xxl.core.indexStructures.Tree.Node.SplitInfo;
import xxl.core.io.converters.BooleanConverter;
import xxl.core.io.converters.Converter;
import xxl.core.io.converters.IntegerConverter;

/**
 * Modification of the BPlusTree with backwards references at the leaf level.
 *
 *
 * @see xxl.core.indexStructures.BPlusTree
 */
public class BPlusLink extends BPlusTree {

	/**
	 * Creates a new BPlusLink with minimum capacity ration 0.5. This is equal
	 * to calling <br/>
	 * <code>
	 * super(blockSize, 0.5f, allowDuplicate)
	 * </code>
	 *
	 * @see xxl.core.indexStructures.BPlusTree#BPlusTree(int, boolean)
	 * @param blockSize
	 *            the size of a page on disk
	 * @param allowDuplicate
	 *            <tt>true</tt>, iff duplicates are allowed, <tt>false</tt>
	 *            otherwise
	 */
	public BPlusLink(final int blockSize, final boolean allowDuplicate) {
		this(blockSize, 0.5f, allowDuplicate);
	}

	/**
	 * Creates a new double-linke BPlusTree with the given blockSize and minimum
	 * capacity ratio.
	 *
	 * @see xxl.core.indexStructures.BPlusTree#BPlusTree(int, double, boolean)
	 * @param blockSize
	 *            the size of a page on disk
	 * @param minCapacityRatio
	 *            the minimum fill ratio of each node
	 * @param allowDuplicate
	 *            <tt>true</tt>, iff duplicates are allowed, <tt>false</tt>
	 *            otherwise
	 */
	public BPlusLink(final int blockSize, final double minCapacityRatio, final boolean allowDuplicate) {
		super(blockSize, minCapacityRatio, allowDuplicate);
	}

	/*
	 * (non-Javadoc)
	 * @see xxl.core.indexStructures.BPlusTree#createNode(int)
	 */
	@Override
	public BPlusLink.Node createNode(final int level) {
		return new Node(level);
	}
	
	@Override
	public IndexEntry createIndexEntry(int parentLevel) {
		return new IndexEntry(parentLevel);
	}
	
	/**
	 * @{inheritDoc}
	 */
	@Override
	public IndexEntry rootEntry() {
		return (IndexEntry) super.rootEntry();
	}
	
	public void setRootEntry(IndexEntry root) {
		rootEntry = root;
	}
	
	public void setRootDescriptor(Descriptor desc) {
		rootDescriptor = desc;
	}

	/**
	 * Creates an index entry for the given node. The node is memory referenced,
	 * thus omitting the tree's access methods when retrieving.
	 *
	 * @param node
	 *            the node to create a temporary index entry for
	 * @return an index entry pointing to the given node
	 */
	protected IndexEntry createTempIndexEntry(final BPlusLink.Node node) {
		final IndexEntry newIndexEntry = new IndexEntry(node.level + 1) {

			@Override
			public Node get() {
				return node;
			}
		};
		return newIndexEntry;
	}

	/*
	 * (non-Javadoc)
	 * @see xxl.core.indexStructures.BPlusTree#createNodeConverter()
	 */
	@Override
	protected NodeConverter createNodeConverter() {
		return new NodeConverter();
	}
	
	public class IndexEntry extends BPlusTree.IndexEntry {

		/**
		 * Creates a new IndexEntry instance
		 * @param parentLevel
		 */
		public IndexEntry(int parentLevel) {
			super(parentLevel);
		}

		/**
		 * @{inheritDoc}
		 */
		@Override
		public Node get(boolean unfix) {
			return (Node) super.get(unfix);
		}

		/**
		 * @{inheritDoc}
		 */
		@Override
		public Node get() {
			return (Node) super.get();
		}

		/**
		 * @{inheritDoc}
		 */
		@Override
		public IndexEntry initialize(SplitInfo splitInfo) {
			return (IndexEntry) super.initialize(splitInfo);
		}

		/**
		 * @{inheritDoc}
		 */
		@Override
		public IndexEntry initialize(Separator separator) {
			return (IndexEntry) super.initialize(separator);
		}

		/**
		 * @{inheritDoc}
		 */
		@Override
		public IndexEntry initialize(Object id, Separator separator) {
			return (IndexEntry) super.initialize(id, separator);
		}

		/**
		 * @{inheritDoc}
		 */
		@Override
		public IndexEntry initialize(Object id) {
			return (IndexEntry) super.initialize(id);
		}

		/**
		 * @{inheritDoc}
		 */
		@Override
		public IndexEntry initialize(Container container, Object id) {
			return (IndexEntry) super.initialize(container, id);
		}

		/**
		 * @{inheritDoc}
		 */
		@Override
		public IndexEntry initialize(Container container, Object id,
			SplitInfo splitInfo) {
			return (IndexEntry) super.initialize(container, id, splitInfo);
		}
	}

	/**
	 * Extension of a BPlusTree node.
	 *
	 * @see xxl.core.indexStructures.BPlusTree.Node
	 */
	public class Node extends BPlusTree.Node {

		/**
		 * Backward reference
		 */
		private IndexEntry previousNeighbor;

		/**
		 * Creates a new node for the given level.
		 *
		 * @param level
		 *            the level the node is created for
		 */
		public Node(final int level) {
			super(level);
		}
		
		public IndexEntry previousNeighbor() {
			return previousNeighbor;
		}
		
		@Override
		public IndexEntry nextNeighbor() {
			return (IndexEntry) nextNeighbor;
		}
		
		public void setNextNeighbor( IndexEntry nn ) {
			nextNeighbor = nn;
		}
		
		public void setPreviousNeighbor( IndexEntry pn ) {
			previousNeighbor = pn;
		}
		

		/**
		 * Returns an <tt>Iterator</tt> of entries whose <tt>Separators</tt>
		 * overlap with the <tt>queryDescriptor</tt>. Initialization of minIndex
		 * and maxIndex similar to
		 * {@link BPlusTree.Node#chooseSubtree(Descriptor descriptor, java.util.Stack path)}
		 *
		 * @param queryDescriptor
		 *            the <tt>KeyRange</tt> describing the query
		 * @return an <tt>Iterator</tt> of entries whose <tt>Separators</tt>
		 *         overlap with the <tt>queryDescriptor</tt>
		 */
		public Iterator<?> queryBackwards(final Descriptor queryDescriptor) {
			final KeyRange qInterval = (KeyRange) queryDescriptor;
			int minIndex = search(qInterval.minBound());
			int maxIndex = rightMostSearch(qInterval.maxBound());
			final List<?> response;
			minIndex = (minIndex >= 0) ? minIndex : (-minIndex - 1 == this.number()) ? -minIndex - 2 : -minIndex - 1;
			maxIndex = (maxIndex >= 0) ? maxIndex : (-maxIndex - 1 == this.number()) ? -maxIndex - 2 : -maxIndex - 1;
			maxIndex = Math.min(maxIndex + 1, number());
			response = this.entries.subList(minIndex, maxIndex);

			return new Iterator<Object>() {

				int index = response.size() - 1;

				@Override
				public boolean hasNext() {
					return this.index >= 0;
				}

				@Override
				public Object next() {
					return response.get(this.index--);
				}

				@Override
				public void remove() {
					response.remove(this.index);
				}
			};
		}
	}
	
	/**
	 * @{inheritDoc}
	 */
	@SuppressWarnings({"rawtypes","unchecked"})
	@Override
	public DoubleCursor<?> rangeQuery(Comparable minKey, Comparable maxKey) {
		return DoubleCursors.wrap(super.rangeQuery(minKey, maxKey));
	}

	/**
	 * @{inheritDoc}
	 */
	@SuppressWarnings("unchecked")
	@Override
	public DoubleCursor<?> query(Descriptor queryDescriptor, int targetLevel) {
		return DoubleCursors.wrap(super.query(queryDescriptor, targetLevel));
	}

	@SuppressWarnings("unchecked")
	@Override
	public DoubleCursor<?> query() {
		return DoubleCursors.wrap(super.query());
	}

	@Override
	protected DoubleCursor<?> query(final BPlusTree.IndexEntry subRootEntry, final KeyRange queryInterval, final int targetLevel) {
		return new QueryCursor(subRootEntry, queryInterval, targetLevel);
	}

	// FIXME: Does not update the path and does not respect the query-range
	@SuppressWarnings("rawtypes")
	protected class QueryCursor extends BPlusTree.QueryCursor implements DoubleCursor {

		/**
		 * Stores if the last operation was forward.
		 */
		private boolean wasForward = true;

		/**
		 * Creates a new <tt>QueryCursor</tt>.
		 *
		 * @param subRootEntry
		 *            the <tt>IndexEntry</tt> which is the root of the subtree
		 *            in which the query has to be executed
		 * @param qInterval
		 *            a <tt>Descriptor</tt> which specifies the query
		 * @param targetLevel
		 */
		public QueryCursor(final BPlusTree.IndexEntry subRootEntry, final KeyRange qInterval, final int targetLevel) {
			super(subRootEntry, qInterval, targetLevel);
		}

		@Override
		public boolean supportsPeekBack() {
			return false;
		}

		@Override
		public Object peekBack() {
			throw new UnsupportedOperationException("PeekBack not supporter!");
		}

		@SuppressWarnings("unchecked")
		@Override
		public Object previous() {
			if ( this.wasForward ) {
				this.wasForward = false;
				this.index--;
				
				computedHasNext = false;
				assignedNext = false;
				
			}
			if ( this.index == 0 ) {
				this.indexEntry = ((Node) this.currentNode).previousNeighbor;
				this.currentNode = this.indexEntry.get();
				this.index = this.currentNode.number();
			}
			if ( this.counterRightShiftDup > 1 ) {
				this.counterRightShiftDup--;
			}
			this.lastNode = (Node) this.currentNode;
			this.lastIndexEntry = (IndexEntry) this.indexEntry;
			this.lastIndex = this.index;
			return ((Node) this.currentNode).getEntry(--this.index);
		}

		@Override
		public boolean hasPrevious() {
			if ( this.wasForward ) {
				this.wasForward = false;
				this.index--;
			}
			return this.currentNode != null && (((Node) this.currentNode).previousNeighbor != null || this.index >= 1);
		}
		
		

		/**
		 * @{inheritDoc}
		 */
		@Override
		public boolean hasNextObject() {
			if ( !this.wasForward ) {
				this.index++;
			}
			this.wasForward = true;
			return super.hasNextObject();
		}

		@Override
		public Object nextObject() {
			if ( !this.wasForward ) {
				this.index++;
			}
			this.wasForward = true;
			return super.nextObject();
		}
	}
	
	/**
	 * @{inheritDoc}
	 */
	@SuppressWarnings({"rawtypes","unchecked"})
	public <TIN,TOUT extends Persistent<TIN>> DoubleCursor<TOUT> rangeQueryWithIds(BinaryFunction<TID,TIN,TOUT> generateOut, Comparable minKey, Comparable maxKey) {
		return new QueryCursorWithIds(generateOut, (IndexEntry)rootEntry, createKeyRange(minKey, maxKey), 0);
	}

	// TODO: Ugly hack
	protected class QueryCursorWithIds<TIN,TOUT extends Persistent<TIN>> extends QueryCursor {

		private final BinaryFunction<TID,TIN,TOUT> generateOut;
		
		/**
		 * Creates a new QueryCursor instance
		 * @param subRootEntry
		 * @param qInterval
		 * @param targetLevel
		 */
		public QueryCursorWithIds(BinaryFunction<TID,TIN,TOUT> generateOut, BPlusTree.IndexEntry subRootEntry, KeyRange qInterval,
				int targetLevel) {
			super(subRootEntry, qInterval, targetLevel);
			this.generateOut = generateOut;
		}

		/**
		 * @{inheritDoc}
		 */
		@SuppressWarnings("unchecked")
		@Override
		public TOUT previous() {
			TIN result = (TIN) super.previous();
			TID id = new TID((Long)lastIndexEntry.id(),lastIndex-1);
			return generateOut.invoke(id, result);
		}

		/**
		 * @{inheritDoc}
		 */
		@SuppressWarnings({ "unchecked" })
		@Override
		public TOUT nextObject() {
			TIN result = (TIN) super.nextObject();
			TID id = new TID((Long)lastIndexEntry.id(),lastIndex);
			return generateOut.invoke(id, result);
		}
	}

	/**
	 * Node converter for the double linked BPlusTree.
	 *
	 * @see xxl.core.indexStructures.BPlusTree.NodeConverter
	 */
	public class NodeConverter extends BPlusTree.NodeConverter {

		/** The serialVersionUID */
		private static final long serialVersionUID = 1L;

		@Override
		protected int headerSize() {
			return 2 * IntegerConverter.SIZE + 2 * BooleanConverter.SIZE + BPlusLink.this.container().getIdSize() * 2;
		}

		/**
		 * Reads a <tt>Node</tt> from the given <tt>DataInput</tt>.
		 *
		 * @param dataInput
		 *            the <tt>DataInput</tt> from which the <tt>Node</tt> has to
		 *            be read
		 * @param object
		 *            is not used
		 * @return the read <tt>Node</tt>
		 * @throws java.io.IOException
		 */
		@Override
		public Object read(final DataInput dataInput, final Object object) throws IOException {
			final boolean readPrev = dataInput.readBoolean();
			Object idEntry = null;
			if ( readPrev ) {
				idEntry = readID(dataInput);
			}
			final Node node = (Node) super.read(dataInput, object);
			if ( readPrev ) {
				node.previousNeighbor = (IndexEntry) createIndexEntry(node.level() + 1);
				node.previousNeighbor.initialize(idEntry);
			}
			else {
				node.previousNeighbor = null;
			}
			return node;
		}

		/**
		 * Writes a given <tt>Node</tt> into a given <tt>DataOutput</tt>.
		 *
		 * @param dataOutput
		 *            the <tt>DataOutput</tt> which the <tt>Node</tt> has to be
		 *            written to
		 * @param object
		 *            the <tt>Node</tt> which has to be written
		 * @throws java.io.IOException
		 */
		@Override
		public void write(final DataOutput dataOutput, final Object object) throws IOException {
			final Node node = (Node) object;
			dataOutput.writeBoolean(node.previousNeighbor != null);
			if ( node.previousNeighbor != null ) {
				writeID(dataOutput, node.previousNeighbor.id());
			}
			super.write(dataOutput, object);
		}

		/**
		 * Reads an ID from the given <tt>DataInput</tt>.
		 *
		 * @param input
		 *            the <tt>DataInput</tt>
		 * @return the read ID
		 * @throws java.io.IOException
		 */
		private Object readID(final DataInput input) throws IOException {
			final Converter<?> idConverter = BPlusLink.this.container().objectIdConverter();
			return idConverter.read(input, null);
		}

		/**
		 * Writes an ID into the given <tt>DataOutput</tt>.
		 *
		 * @param output
		 *            the <tt>DataOutput</tt>
		 * @param id
		 *            the ID which has to be written
		 * @throws java.io.IOException
		 */
		private void writeID(final DataOutput output, final Object id) throws IOException {
			@SuppressWarnings("unchecked")
			final Converter<Object> idConverter = BPlusLink.this.container().objectIdConverter();
			idConverter.write(output, id);
		}
	}
}
