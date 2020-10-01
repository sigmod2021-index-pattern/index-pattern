package xxl.core.indexStructures;

import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Stack;

import sigmod2021.db.core.primaryindex.impl.legacy.SchemaManager;
import sigmod2021.db.core.wal.Log;
import sigmod2021.event.*;
import sigmod2021.event.impl.SimpleEvent;
import xxl.core.collections.MapEntry;
import xxl.core.collections.containers.Container;
import xxl.core.functions.Function;
import xxl.core.io.converters.LongConverter;
import xxl.core.io.converters.MeasuredConverter;
import xxl.core.util.Pair;

/**
 * Augments a BPlusTree with aggregate values, i.e. sum, min, max and count.
 * This implementation uses double values for all aggregates.
 *
 * To determine the values to be aggregated, this implementation requires a
 * schema. All numeric values of the schema are aggregated.
 *
 */
public class FastAggregatedBPlusTree extends BPlusLink implements AggregateIndex<FastAggregatedBPlusTree.Node> {

	/**
	 * Level for leaf nodes.
	 */
	private static final int			LEAF_LEVEL	= 0;

	private final TimeRepresentation timeRepresentation;

	/**
	 * The schema of the tree.
	 */
	protected EventSchema schema;

	/**
	 * Stores all aggregates.
	 */
	private int[]						aggregateColumns;

	/**
	 * Stores the index of the time stamp.
	 */
	//	private final int			timeIndex;

	/**
	 * The WAL.
	 */
	private final Log<Event> log;

	/**
	 * Record for aggregation
	 */
	public static class AggregationEntry implements AggregateIndex.Aggregation {

		protected double	sum;
		protected double	min;
		protected double	max;
		protected long		count;

		public AggregationEntry() {
			this.sum = 0;
			this.min = Double.POSITIVE_INFINITY;
			this.max = Double.NEGATIVE_INFINITY;
			this.count = 0L;
		}

		/**
		 * Returns all values in the following order: sum, min, max
		 *
		 * @return the aggregate values in the order sum,min,max
		 */
		@Override
		public Number[] getValues() {
			return new Number[] { this.sum, this.min, this.max, this.count };
		}

		/**
		 * Creates a copy of this aggregate.
		 *
		 * @return the deep copy
		 */
		@Override
		protected AggregationEntry clone() {
			final AggregationEntry result = new AggregationEntry();
			result.update(this);
			return result;
		}

		/**
		 * Updates this aggregate and adds the given number
		 *
		 * @param value
		 *            the value to be added
		 */
		protected void update(final Number value) {
			this.sum = this.sum + value.doubleValue();
			if ( value.doubleValue() > this.max ) {
				this.max = value.doubleValue();
			}
			if ( value.doubleValue() < this.min ) {
				this.min = value.doubleValue();
			}
			this.count++;
		}

		/**
		 * Updates this aggregate and adds the given one
		 *
		 * @param value
		 *            the value to be added
		 */
		public void update(final AggregationEntry value) {
			this.sum = this.sum + value.sum;
			if ( value.max > this.max ) {
				this.max = value.max;
			}
			if ( value.min < this.min ) {
				this.min = value.min;
			}
			this.count += value.count;
		}

		/**
		 * Combines this aggregate and the given one to a new aggregate
		 *
		 * @param value
		 *            the value to be combined with this one
		 * @return the combined aggregate
		 */
		public AggregationEntry combine(final AggregationEntry value) {
			return clone().combine(value);
		}

		@Override
		public void update(final AggregateIndex.Aggregation agg) {
			update((AggregationEntry) agg);
		}

		@Override
		public String toString() {
			return "s:" + this.sum + ",min:" + this.min + ",max:" + this.max + ",count:" + this.count;
		}
	}

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
	public FastAggregatedBPlusTree(final EventSchema schema, final TimeRepresentation rep, final int blockSize,
		final boolean allowDuplicate, final Log<Event> log) {
		this(schema, rep, blockSize, allowDuplicate, 0.5f, log);
	}

	/**
	 * Creates a new aggregated BPlusTree with the given blockSize and minimum
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
	public FastAggregatedBPlusTree(final EventSchema schema, final TimeRepresentation rep, final int blockSize,
		final boolean allowDuplicate,
		final double minCapacityRatio, final Log<Event> log) {
		super(blockSize, minCapacityRatio, allowDuplicate);
		this.schema = schema;
		this.timeRepresentation = rep;
		this.nodeConverter = createNodeConverter();
		this.log = log;
		initAggregateColumns();
	}

	@Override
	protected FastAggregatedBPlusTree initialize(final Function getKey, final MeasuredConverter keyConverter,
		final MeasuredConverter dataConverter, final Function createSeparator, final Function createKeyRange,
		final Function getSplitMinRatio, final Function getSplitMaxRatio) {
		super.initialize(getKey, keyConverter, dataConverter, createSeparator, createKeyRange, getSplitMinRatio,
			getSplitMaxRatio);

		final int spaceIndex = this.BLOCK_SIZE - ((NodeConverter) this.nodeConverter).headerSize();
		final int spaceLeaf = this.BLOCK_SIZE - ((NodeConverter) this.nodeConverter).leafHeaderSize();
		this.B_IndexNode = spaceIndex / ((NodeConverter) this.nodeConverter).indexEntrySize();
		this.B_LeafNode = spaceLeaf / ((NodeConverter) this.nodeConverter).leafEntrySize();
		this.D_IndexNode = (int) (this.minCapacityRatio * this.B_IndexNode);
		this.D_LeafNode = (int) (this.minCapacityRatio * this.B_LeafNode);
		return this;
	}

	/**
	 * Initializes all aggregates.
	 */
	protected void initAggregateColumns() {
		final ArrayList<Integer> indices = new ArrayList<>();
		for (int pos = 0, i = 0; i < this.schema.getNumAttributes(); i++) {
			final Attribute att = this.schema.getAttribute(i);
			if ( att.getType().isNumeric() &&
				att.getProperty("index") != null &&
				att.getProperty("index").equalsIgnoreCase("true") ) {
				indices.add(pos++, i);
			}
		}
		this.aggregateColumns = new int[indices.size()];
		for (int i = 0; i < this.aggregateColumns.length; i++) {
			this.aggregateColumns[i] = indices.get(i);
		}
	}

	@Override
	public Node createNode(final int level) {
		return new Node(level);
	}

	@Override
	protected NodeConverter createNodeConverter() {
		return new NodeConverter();
	}

	@Override
	public IndexEntry createIndexEntry(final int parentLevel) {
		return new IndexEntry(parentLevel);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	protected IndexEntry createTempIndexEntry(final BPlusLink.Node node) {
		final IndexEntry newIndexEntry = new IndexEntry(node.level + 1) {

			@Override
			public Node get(final boolean unfix) {
				return (Node) node;
			}

		};
		return newIndexEntry;
	}

	/**
	 * This class extends the index entries of the <tt>BPlusTree</tt> (i.e. the
	 * entries of the non-leaf nodes). The aggregated BPlusTree adds [min-max]
	 * aggregate values for each attribute.
	 *
	 * @see xxl.core.indexStructures.BPlusTree.IndexEntry
	 */
	public class IndexEntry extends BPlusLink.IndexEntry {

		AggregationEntry[] aggregates;

		public IndexEntry(final int parentLevel) {
			super(parentLevel);
			this.aggregates = new AggregationEntry[0];
		}

		public IndexEntry(final BPlusTree.IndexEntry entry) {
			super(entry.parentLevel());
			this.aggregates = new AggregationEntry[0];
			initialize(entry.id());
		}

		public Aggregation getAggregation(final String attribute) {
			int index = getIndexForAttribute(FastAggregatedBPlusTree.this.schema.getAttributeIndex(attribute));
			return this.aggregates[index];
		}

		/**
		 * Returns the min max interval of the given interval.
		 *
		 * @param attribute
		 *            the attribute`s name
		 * @return the min max interval or null, if there is no min max interval
		 */
		public Pair<? extends Number, ? extends Number> getMinMax(final String attribute) {
			int index;
			try {
				index = getIndexForAttribute(FastAggregatedBPlusTree.this.schema.getAttributeIndex(attribute));
				return new Pair<>(this.aggregates[index].min, this.aggregates[index].max);
			}
			catch (SchemaException e) {
				return null;
			}

		}

		/**
		 * Initializes this index entry with the node´s aggregates
		 *
		 * @param node
		 *            the node
		 * @return this index entry
		 */
		public IndexEntry initialize(final Node node) {
			this.aggregates = new AggregationEntry[FastAggregatedBPlusTree.this.aggregateColumns.length];
			for (int i = 0; i < FastAggregatedBPlusTree.this.aggregateColumns.length; i++) {
				this.aggregates[i] = node.aggregates[i].clone();
			}
			return this;
		}

		@Override
		public IndexEntry initialize(final Tree.Node.SplitInfo splitInfo) {
			final IndexEntry ie = (IndexEntry) super.initialize(splitInfo);
			final Node n = (Node) splitInfo.newNode();
			return ie.initialize(n);
		}

		/**
		 * {@inheritDoc}
		 */
		@Override
		public Node get(final boolean unfix) {
			return (Node) super.get(unfix);
		}

		/**
		 * {@inheritDoc}
		 */
		@Override
		public Node get() {
			return (Node) super.get();
		}

		public String plot() {
			final String nl = String.format("%n");
			StringBuilder sb = new StringBuilder();
			sb.append("IndexEntry [").append(nl);
			sb.append("  id       : ").append(this.id()).append(nl);
			sb.append("  separator: ").append(this.separator()).append(nl);

			for (Attribute att : FastAggregatedBPlusTree.this.schema) {
				int aggIdx = getIndexForAttribute(FastAggregatedBPlusTree.this.schema.getAttributeIndex(att.getName()));
				if ( aggIdx < 0 )
					continue;
				AggregationEntry agg = this.aggregates[aggIdx];
				sb.append("  ").append(att.getName()).append(": interval=[").append(String.format("%.4f", agg.min))
					.append(", ").append(String.format("%.4f", agg.max)).append("], sum=").append(agg.sum)
					.append(", count=").append(agg.count).append(nl);
			}
			sb.append("]");

			return sb.toString();
		}
	}

	/**
	 * Extension of a BPlusTree node.
	 *
	 * @see xxl.core.indexStructures.BPlusTree.Node
	 */
	public class Node extends BPlusLink.Node {

		/** Stores the node`s aggregates */
		protected AggregationEntry[]	aggregates;
		/** Indicates whether this node has been modified after creation */
		protected boolean				modified;
		/** The lsn of this node */
		protected long					lsn;

		public Node(final int level) {
			super(level);
			this.aggregates = new AggregationEntry[FastAggregatedBPlusTree.this.aggregateColumns.length];

			initAggregates();
		}

		/**
		 * Returns whether this node has been modified by a split/insert
		 * operation.
		 *
		 * @return true if this node has been modified, false otherwise
		 */
		public boolean isModified() {
			return this.modified;
		}

		/**
		 * @{inheritDoc}
		 */
		@Override
		public IndexEntry previousNeighbor() {
			return (IndexEntry) super.previousNeighbor();
		}

		/**
		 * @{inheritDoc}
		 */
		@Override
		public IndexEntry nextNeighbor() {
			return (IndexEntry) super.nextNeighbor();
		}

		/**
		 * Inserts a new entry into this <tt>Node</tt> at the suitable position.
		 * The position is found using a binary search.
		 * {@link xxl.core.indexStructures.BPlusTree.Node#binarySearch(Comparable)}
		 *
		 * @param entry
		 *            the new entry which has to be inserted
		 * @exception IllegalArgumentException
		 *                in normal mode if the key already exists in the node.
		 *
		 */
		@Override
		protected void grow(final Object entry) {
			super.grow(entry);
			recalcAggregates();
			// Update LSN for this node
			this.lsn = FastAggregatedBPlusTree.this.log.getLSN();
		}

		@Override
		protected void grow(final Object entry, final Stack path) {
			grow(entry);
			if ( this.level == LEAF_LEVEL ) {
				FastAggregatedBPlusTree.this.log.insert((Long) indexEntry(path).id(), (Event) entry);
			}
		}

		@Override
		protected Tree.Node.SplitInfo split(final Stack path) {
			final Tree.Node.SplitInfo splitInfo = super.split(path);
			final Node splittedNode = (Node) node(path); // The node that is split
			final Node overflowNode = this; // The node that is used as overflow node

			// The overflowNode`s entry list is filled by the super class
			// directly (without calling updateAggregates for each entry)
			// => recalculate the aggregates
			overflowNode.recalcAggregates();
			//Update splitted node
			splittedNode.recalcAggregates();
			splittedNode.modified = true; // The split node is modified

			// Update aggregates in index entry
			((IndexEntry) indexEntry(path)).initialize(splittedNode);
			overflowNode.setPreviousNeighbor(createIndexEntry(overflowNode.level).initialize(indexEntry(path).id()));
			return splitInfo;
		}

		@Override
		protected Tree.Node.SplitInfo redressOverflow(final Stack path, final Tree.Node parentNode,
			final List newIndexEntries) {
			final Tree.Node.SplitInfo result = super.redressOverflow(path, parentNode, newIndexEntries);
			// Update previous neighbor pointing to old node after splitting
			final Node node = (Node) result.newNode();
			if ( node.nextNeighbor() != null ) {
				final Node n = (Node) node.nextNeighbor().get();
				final IndexEntry entry = (IndexEntry) newIndexEntries.get(newIndexEntries.size() - 1);
				n.previousNeighbor().initialize(entry.id());
				container().update(node.nextNeighbor().id(), n, false);
			}
			return result;
		}

		/**
		 * Clears all aggregates
		 */
		protected void initAggregates() {
			for (int i = 0; i < this.aggregates.length; i++) {
				this.aggregates[i] = new AggregationEntry();
			}
		}

		/**
		 * Recalculates all aggregates of this node
		 */
		protected void recalcAggregates() {
			initAggregates();
			for (int i = 0; i < this.entries.size(); i++) {
				recoverAggregates(this.entries.get(i), this);
			}
		}

		/**
		 * Update the current aggregates with the given entry.
		 *
		 * @param entry
		 *            the entry whose values should be used to update the
		 *            aggregates
		 *
		 */
		protected void updateAggregates(final Object entry) {
			updateAggregates(entry, true);
		}

		/**
		 * Inserts the given entry into the node
		 *
		 * @param entry
		 *            the entry to be inserted
		 */
		protected void updateAggregates(final Object entry, final boolean update) {

			if ( this.level == LEAF_LEVEL ) { // Calculate aggregate
				final Event tuple = (Event) entry;
				for (int i = 0; i < this.aggregates.length; i++) {
					this.aggregates[i]
						.update(tuple.get(FastAggregatedBPlusTree.this.aggregateColumns[i], Number.class));
				}
			}
			else if ( update ) {
				final IndexEntry ie = (IndexEntry) entry;

				for (int i = 0; i < this.aggregates.length; i++) { //Iterates the aggregated attributes
					this.aggregates[i].update(ie.aggregates[i]);
				}
			}
		}

		/**
		 * Merges the aggregates with the aggregates from the given node.
		 *
		 * @param node
		 *            the node which aggregates should be included
		 */
		protected void updateAggregates(final Node node) {
			for (int i = 0; i < this.aggregates.length; i++) { //Iterates the aggregated attributes
				this.aggregates[i].update(node.aggregates[i]);
			}
		}

		/**
		 * Inserts the given entry into the node
		 *
		 * @param entry
		 *            the entry to be inserted
		 */
		public void recoverAggregates(final Object entry, final Node node) {

			if ( this.level == LEAF_LEVEL ) { // Calculate aggregate
				final Event tuple = (Event) entry;
				for (int i = 0; i < this.aggregates.length; i++) {
					this.aggregates[i]
						.update(tuple.get(FastAggregatedBPlusTree.this.aggregateColumns[i], Number.class));
				}
			}
			else { // recalculate aggregates  from index entries
				final IndexEntry ie = (IndexEntry) entry;
				for (int i = 0; i < this.aggregates.length; i++) { //Iterates the aggregated attributes
					this.aggregates[i].update(ie.aggregates[i]);
				}
			}
		}

		@Override
		protected Collection redressOverflow(final Stack path) {
			return super.redressOverflow(path);
		}

		@Override
		protected Tree.Node.SplitInfo initialize(final Object entry) {
			return super.initialize(entry);
		}

		@Override
		public String toString() {
			return Arrays.toString(this.aggregates);
		}
	}

	/**
	 * Node converter for the double linked BPlusTree.
	 *
	 * @see xxl.core.indexStructures.BPlusTree.NodeConverter
	 */
	public class NodeConverter extends BPlusLink.NodeConverter {

		/** The serialVersionUID */
		private static final long	serialVersionUID	= 1L;
		private static final int	MODIFIED_FLAG_SIZE	= 1;
		private static final int	LSN_ENTRY_SIZE		= 8;

		@Override
		protected int headerSize() {
			return super.headerSize() + MODIFIED_FLAG_SIZE + LSN_ENTRY_SIZE;
		}

		protected int leafHeaderSize() {
			return super.headerSize() + MODIFIED_FLAG_SIZE + LSN_ENTRY_SIZE;
		}

		@Override
		protected int leafEntrySize() {
			return super.leafEntrySize();
		}

		@Override
		protected int indexEntrySize() {
			final int result = FastAggregatedBPlusTree.this.aggregateColumns.length * (4 * 8);
			return super.indexEntrySize() + result;
		}

		/**
		 * Returns the size in bytes of the given string after serialization.
		 *
		 * @param s
		 *            the string to be serialized
		 * @return the size of the serialized string
		 */
		private int getSize(final String s) {
			final ByteArrayOutputStream bos = new ByteArrayOutputStream();
			try {
				new DataOutputStream(bos).writeUTF(s);
			}
			catch (final IOException e) {
			}
			return bos.size();
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
			final Node node = (Node) super.read(dataInput, object);
			// read split flag
			node.modified = dataInput.readBoolean();
			// Read LSN
			node.lsn = dataInput.readLong();
			node.recalcAggregates();
			return node;
		}

		@Override
		protected void readEntries(final DataInput input, final BPlusTree.Node node, final int number)
			throws IOException {
			if ( node.level == 0 ) {
				readLeafNode(input, node, number);
			}
			else {
				final List<Object> entries = new ArrayList<>(number);
				for (int i = 0; i < number; i++) {
					entries.add(readIndexEntry(input, node.level, ((Node) node).aggregates.length));
				}
				node.initialize(node.level, entries);
			}
		}

		@Override
		protected void writeEntries(final DataOutput output, final BPlusTree.Node node) throws IOException {
			if ( node.level == 0 ) {
				writeLeafNode(output, node);
			}
			else {
				final Iterator<?> entries = node.entries();
				while (entries.hasNext()) {
					final Object entry = entries.next();
					writeIndexEntry(output, (IndexEntry) entry);
				}
			}
		}

		private void readLeafNode(final DataInput input, final BPlusTree.Node node, final int number)
			throws IOException {
			//			final List<PersistentEvent> objects = new ArrayList<>(number);
			Object[][] payloads = new Object[number][FastAggregatedBPlusTree.this.schema.getNumAttributes()];
			long[] tstart = new long[number];
			long[] tend = new long[number];

			for (int a = 0; a < FastAggregatedBPlusTree.this.schema.getNumAttributes(); a++) {
				final MeasuredConverter<Object> converter =
					SchemaManager.getObjectConverter(FastAggregatedBPlusTree.this.schema.getAttribute(a));
				for (int i = 0; i < number; i++) {
					payloads[i][a] = converter.read(input);
				}
			}
			for (int i = 0; i < number; i++) {
				tstart[i] = LongConverter.DEFAULT_INSTANCE.readLong(input);
				tend[i] = tstart[i] + 1;
			}
			if ( timeRepresentation == TimeRepresentation.INTERVAL ) {
				for (int i = 0; i < number; i++) {
					tend[i] = LongConverter.DEFAULT_INSTANCE.readLong(input);
				}
			}
			List<Event> events = new ArrayList<>();
			for (int i = 0; i < number; i++) {
				// TODO: Find a way to deliver persistent events here
				events.add(new SimpleEvent(
					payloads[i],
					tstart[i],
					tend[i]));
			}

			node.initialize(node.level, events);
		}

		private void writeLeafNode(final DataOutput output, final BPlusTree.Node node) throws IOException {
			for (int a = 0; a < FastAggregatedBPlusTree.this.schema.getNumAttributes(); a++) {
				final MeasuredConverter<Object> converter = SchemaManager
					.getObjectConverter(FastAggregatedBPlusTree.this.schema.getAttribute(a));
				for (int i = 0; i < node.number(); i++) {
					converter.write(output, ((Event) node.getEntry(i)).get(a));
				}
			}
			for (int i = 0; i < node.number(); i++)
				LongConverter.DEFAULT_INSTANCE.write(output, ((Event) node.getEntry(i)).getT1());

			if ( timeRepresentation == TimeRepresentation.INTERVAL ) {
				for (int i = 0; i < node.number(); i++)
					LongConverter.DEFAULT_INSTANCE.write(output, ((Event) node.getEntry(i)).getT2());
			}
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
			super.write(dataOutput, node);
			// TODO: write split flag and LSN more efficiently and only for leaf nodes!
			dataOutput.writeBoolean(node.modified);
			// write LSN
			dataOutput.writeLong(node.lsn);
		}

		protected IndexEntry readIndexEntry(final DataInput input, final int parentLevel, final int attributeCount)
			throws IOException {
			final IndexEntry indexEntry = new IndexEntry(super.readIndexEntry(input, parentLevel));
			indexEntry.aggregates = new AggregationEntry[attributeCount];
			for (int i = 0; i < attributeCount; i++) {
				final AggregationEntry agg = new AggregationEntry();
				agg.sum = input.readDouble();
				agg.min = input.readDouble();
				agg.max = input.readDouble();
				agg.count = input.readLong();
				indexEntry.aggregates[i] = agg;
			}
			return indexEntry;
		}

		@Override
		protected void writeIndexEntry(final DataOutput output, final BPlusTree.IndexEntry entry) throws IOException {
			super.writeIndexEntry(output, entry);
			final IndexEntry e = (IndexEntry) entry;
			for (int i = 0; i < e.aggregates.length; i++) {
				final AggregationEntry agg = e.aggregates[i];
				output.writeDouble(agg.sum);
				output.writeDouble(agg.min);
				output.writeDouble(agg.max);
				output.writeLong(agg.count);
			}
		}
	}

	@Override
	public AggregationEntry getAggregate(final String attribute, final Comparable minKey, final Comparable maxKey,
		final Node node, Long nodeLeft) {
		try {
			int i = this.schema.getAttributeIndex(attribute);
			return getAggregate(i, createKeyRange(minKey, maxKey), node, nodeLeft);
		}
		catch (SchemaException e) {
			return null;
		}
	}

	@Override
	public AggregationEntry getAggregate(final String attribute, final Comparable minKey, final Comparable maxKey,
		Comparable globalMinKey) {
		IndexEntry ie = this.rootEntry();
		return getAggregate(attribute, minKey, maxKey, ie.get(), (Long) globalMinKey);
	}

	/**
	 * 
	 * @param attributeIndex
	 * @param range
	 *            The key-range interval [min,max], both inclusive
	 * @param node
	 *            the node
	 * @param nodeRange
	 *            the node's left key-bound (inclusive)
	 * @return
	 */
	private AggregationEntry getAggregate(final int attributeIndex, final KeyRange range, final Node node,
		final Long nodeLeft) {

		Long queryLeft = (Long) range.minBound();
		Long queryRight = (Long) range.maxBound();

		final AggregationEntry agg = new AggregationEntry();
		if ( node.level() == LEAF_LEVEL ) {
			// calculate aggregate in leaf

			for (int i = 0; i < node.number(); i++) {
				final Event event = (Event) (node.getEntry(i));
				if ( queryLeft <= event.getT1() &&
					queryRight >= event.getT1() ) {
					agg.update(event.get(attributeIndex, Number.class));
				}
			}
		}
		else {
			Long leftBound = nodeLeft;
			for (int i = 0; i < node.number(); i++) {
				final IndexEntry subEntry = (IndexEntry) node.getEntry(i);
				final Long rightBound = (Long) subEntry.separator().sepValue();

				// Fully within
				if ( queryLeft <= leftBound &&
					queryRight >= rightBound ) {
					agg.update(subEntry.aggregates[getIndexForAttribute(attributeIndex)]);
				}
				// If intersects: down one level
				else if ( !(queryLeft > rightBound || queryRight < leftBound) ) {
					Node subNode = subEntry.get();
					agg.update(getAggregate(attributeIndex, range, subNode, leftBound));
				}
				// Break, if we left the query-range
				else if ( queryRight < leftBound ) {
					break;
				}
				leftBound = rightBound + 1;
			}
		}
		return agg;
	}

	/**
	 * Returns the aggregate index for the attribute with the given index.
	 *
	 * @param attributeIndex
	 *            the index of the requested attribute in the schema
	 * @return the index of the aggregate in the aggregates array
	 */
	private int getIndexForAttribute(final int attributeIndex) {
		for (int i = 0; i < this.aggregateColumns.length; i++) {
			if ( this.aggregateColumns[i] == attributeIndex )
				return i;
		}
		return -1;
	}

	@Override
	protected Map.Entry grow(final Object entry) {
		final Node rootNode = createNode(height());
		final Tree.Node.SplitInfo splitInfo = rootNode.initialize(entry);
		rootNode.updateAggregates(entry, true);
		final Container container = splitInfo.determineContainer();
		final Object id = container.insert(rootNode, false);

		setRootEntry(createIndexEntry(height() + 1).initialize(container, id, splitInfo));
		return new MapEntry(this.rootEntry(), rootNode);
	}

	@Override
	protected Tree.IndexEntry up(final Stack path, final boolean unfix) {
		updatePathAggregate(path);
		final Tree.IndexEntry result = super.up(path, unfix);
		return result;
	}

	@Override
	protected void post(final Stack path) {
		updatePathAggregate(path);
		if ( !path.isEmpty() ) {
			while (!((Node) node(path)).redressOverflow(path).isEmpty()) {
				;
			}
			while (!path.isEmpty()) {
				update(path); // All nodes have to be updated!
				up(path);
			}
		}
	}

	private void updatePathAggregate(final Stack path) {
		if ( !path.isEmpty() ) {
			((Node) node(path)).recalcAggregates();
			((IndexEntry) indexEntry(path)).initialize((Node) node(path));
		}
	}

	@Override
	protected void update(final Stack path) {
		updatePathAggregate(path);
		super.update(path);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public IndexEntry rootEntry() {
		return (IndexEntry) super.rootEntry();
	}

}
