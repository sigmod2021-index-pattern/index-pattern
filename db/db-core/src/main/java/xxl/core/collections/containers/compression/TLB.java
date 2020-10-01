package xxl.core.collections.containers.compression;

import java.io.ByteArrayInputStream;
import java.io.DataInput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.NoSuchElementException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import xxl.core.collections.containers.compression.buffer.BlockBuffer;
import xxl.core.io.UnsafeDataInput;
import xxl.core.io.UnsafeDataOutput;
import xxl.core.io.raw.RawAccess;
import xxl.core.util.Pair;

/**
 * Translation look-ahead buffer for translating logical ids into physical ids.
 *
 * This implementation allows variable-size records and only needs one single
 * file for each container. The address information is stored interleaved. The
 * root of the buffer is always kept in memory.
 *
 */
public class TLB {

	/**
	 * The logger.
	 */
	private static final Logger	logger					= LoggerFactory.getLogger(TLB.class);

	public static final long	UNMAPPED_ID_PHYSICAL    = -1L;

	/**
	 * Defines the size of a TLB entry header.
	 */
	private static final int	TLB_HEADER_SIZE			= 32;																		 // tag, level, previous entry, previous parent entry, next macro block id, entry count
	/** The tag indicating a TLB entry */
	public static final short	TLB_TAG					= -5;

	/** Size of a block on disk */
	private final int			blockSize;
	/** The number IDs per TLB entry */
	private final int			bufferSize;
	/**
	 * The address buffer for writing, always keeping the the last TLB entry for
	 * each level
	 */
	List<TLBEntry>				writeBuffer;
	/**
	 * The container file to append the TLB entries
	 */
	//    private RandomAccessFile raf;
	private final RawAccess		raf;
	/**
	 * A general read buffer (also for upper levels)
	 */
	List<TLBEntry>				readBuffer;
	/**
	 * The buffer for leaf TLB entries (containing 2 entries at max). The last 2
	 * leaf entries are buffered because the FastCompressionContainer always
	 * needs to lookup the next node to determine the reading offset.
	 */
	private final TLBLeafBuffer	leafBuffer;

	/**
	 * The block buffer to data load from
	 */
	private BlockBuffer			blockBuffer;

	/**
	 * An entry in the tlb buffer
	 */
	protected static class TLBEntry {

		protected long				mapID;
		protected short				level;
		protected long				previousEntry		= -1;		 // the position of the previous entry in the same level
		protected long				previousParentEntry	= -1;		 // the position of the previous entry in the upper level
		protected long				nextMacroBlockID	= -1;
		protected ArrayList<Long>	entries;
		private final int			bufferSize;

		public TLBEntry(final int bufferSize, final int level) {
			this(bufferSize);
			this.level = (short) level;
		}

		public TLBEntry(final int bufferSize) {
			this.bufferSize = bufferSize;
			this.entries = new ArrayList<>(bufferSize);
		}

		boolean isFull() {
			return this.entries.size() == this.bufferSize;
		}
	}

	/**
	 * A buffer for leaf TLB entries.
	 */
	private class TLBLeafBuffer {

		private final TLBEntry[]	entryBuffer;
		private final long[]		idBuffer;
		private int					index;

		public TLBLeafBuffer() {
			this.entryBuffer = new TLBEntry[2];
			this.idBuffer = new long[] { -1, -1 };
			this.index = 0;
		}

		public void add(final TLBEntry leafEntry) {
			this.entryBuffer[this.index] = leafEntry;
			this.idBuffer[this.index] = leafEntry.mapID;
			this.index = (this.index + 1) % this.entryBuffer.length;
		}

		public boolean contains(final long mapID) {
			return this.idBuffer[0] == mapID || this.idBuffer[1] == mapID;
		}

		public TLBEntry get(final long mapID) {
			if ( this.idBuffer[0] == mapID )
				return this.entryBuffer[0];
			else if ( this.idBuffer[1] == mapID )
				return this.entryBuffer[1];
			else
				return null;
		}
	}

	/**
	 * Creates a new TLB for the given random access file and the given block
	 * size
	 *
	 * @param raf
	 *            the underlying random access file
	 * @param blockSize
	 *            the block size
	 */
	public TLB(final RawAccess raf, final int blockSize) {
		this.raf = raf;
		this.blockSize = blockSize;
		this.bufferSize = (blockSize - TLB_HEADER_SIZE) / 8;
		// Init write buffer
		this.writeBuffer = new ArrayList<>(2);
		this.writeBuffer.add(new TLBEntry(this.bufferSize, 0));
		this.writeBuffer.add(new TLBEntry(this.bufferSize, 1));
		// Init read buffer
		this.readBuffer = new ArrayList<>(2);
		this.readBuffer.add(this.writeBuffer.get(0));
		this.readBuffer.add(this.writeBuffer.get(1));
		this.leafBuffer = new TLBLeafBuffer();
	}

	/**
	 * Adds a new mapping for a logical address. The logical address is implicit
	 * because addresses are successive. The method returns the number of bytes
	 * written to the file.
	 *
	 * @param physicalAddress
	 *            the new physical address to be added
	 * @param containerPosition
	 *            the last position in the container
	 * @return the number of bytes written for TLB entries
	 */
	public long addMapping(final long physicalAddress, final long containerPosition, final long lid,
		final long macroBlockId) throws IOException {
		return addMapping(physicalAddress, containerPosition, lid, 0, macroBlockId);
	}

	/**
	 * Inserts the given physical address recursively (bottom to top) into the
	 * TLB.
	 *
	 * @param physicalAddress
	 *            the physical address
	 * @param containerPosition
	 *            the position in the container
	 * @param level
	 *            the level
	 * @return the length of the written entries
	 * @throws IOException
	 */
	private long addMapping(final long physicalAddress, final long containerPosition, final long lid, final int level,
		final long macroBlockId) throws IOException {
		if ( level >= this.writeBuffer.size() ) {
			this.writeBuffer.add(level, new TLBEntry(this.bufferSize, level));
			this.readBuffer.add(level, this.writeBuffer.get(level));
		}
		if ( level == 0 && (lid == -1L || lid == getLastMappingId() + 1) ) {
			this.writeBuffer.get(level).entries.add(physicalAddress);
		}
		else if ( level == 0 && notInPath(lid, level) ) {
			// Out of order update (update of a previously allocated TLB)
			final Pair<Long, TLBEntry> entry = getLeafEntry(lid);
			final int index = (int) (lid % this.bufferSize);
			entry.getElement2().entries.set(index, physicalAddress);
			// Update in the container
			final byte[] block = serialize(entry.getElement2(), 0);
			this.raf.write(block, entry.getElement1() / this.blockSize);
			this.blockBuffer.replace(entry.getElement1() / this.blockSize, block); // Update sequential read buffer (if necessary)
			this.blockBuffer.replaceTLB(block, level, (int) (getMapID(lid, level + 1) % this.bufferSize)); // Update TLB buffer (if necessary)
			logger.trace("Writing TLB block [" + entry.getElement2().level + "] on id " + lid + " at " +
				entry.getElement1() + " (Block " + (entry.getElement1() / this.blockSize) + ")");
			return 0;
		}
		else {
			// Calculate index from lid
			final int index = (int) (lid % this.bufferSize);
			if ( level > 0 || index == this.writeBuffer.get(level).entries.size() ) {
				this.writeBuffer.get(level).entries.add(physicalAddress);
			}
			else {
				this.writeBuffer.get(level).entries.set(index, physicalAddress);
			}
		}
		if ( this.writeBuffer.get(level).isFull() ) {
			this.writeBuffer.get(level).nextMacroBlockID = macroBlockId;
			final byte[] serialized = serialize(this.writeBuffer.get(level), level);
			// Keeps the position of the last mapping entry for linking

			this.raf.write(serialized, containerPosition / this.blockSize);
			this.blockBuffer.replaceTLB(serialized, level, (int) (getMapID(lid, level + 1) % this.bufferSize)); // Update TLB buffer (if necessary)
			logger.trace("Writing TLB block [" + level + "] on id " + lid + " at " + containerPosition + " (Block " +
				(containerPosition / this.blockSize) + ")");
			this.writeBuffer.get(level).entries.clear();
			this.writeBuffer.get(level).mapID++;
			this.writeBuffer.get(level).nextMacroBlockID = -1L;

			// Set previous id
			this.writeBuffer.get(level).previousEntry = containerPosition;
			// Set previous id for upper level => set for lower level
			if ( level > 0 ) {
				this.writeBuffer.get(level - 1).previousParentEntry = containerPosition;
			}

			return serialized.length + addMapping(containerPosition, containerPosition + serialized.length,
				lid / this.bufferSize, level + 1, macroBlockId);
		}
		return 0;
	}

	/**
	 * Checks if the path of the write buffer does not contain the mapping for
	 * the requested id.
	 *
	 * @param id
	 *            the requested id
	 * @param level
	 *            the level
	 * @return true, if the path does not contain the node, false otherwise
	 */
	private boolean notInPath(final long id, final int level) {
		for (int i = this.writeBuffer.size() - 1; i >= level; i--) {
			if ( getMapID(id, level + 1) < this.writeBuffer.get(i).mapID )
				return true;
		}
		return false;
	}

	/**
	 * Removes the last mapping from the TLB. This method propagates also to
	 * upper levels.
	 *
	 * @throws IOException
	 *             if an error occurs
	 */
	public void removeLastMapping() throws IOException {
		removeLastMapping(0);
	}

	/**
	 * Removes the last mapping from this TLB. If the current entry is empty,
	 * the previous entry is loaded (if a previous entry exists) and its last
	 * entry is removed.
	 *
	 * @param level
	 *            the level in which to remove
	 * @throws IOException
	 *             if an error occurs
	 */
	private void removeLastMapping(final int level) throws IOException {
		if ( this.writeBuffer.get(level).entries.size() == 0 ) {
			// Switch to previous entry
			this.writeBuffer.set(level, deserialize(this.writeBuffer.get(level).previousEntry, level));
			// Remove entry from parent
			if ( level < this.writeBuffer.size() - 1 ) {
				removeLastMapping(level + 1);
			}
		}
		// Remove the last entry
		this.writeBuffer.get(level).entries.remove(this.writeBuffer.get(level).entries.size() - 1);
	}

	/**
	 * Returns the number of TLB blocks required for the given ID.
	 *
	 * @param id
	 *            the id
	 * @return the number of required TLB blocks
	 */
	public long getMapBlockCount(final long id) {
		long result = 0;
		for (int i = 1; i < this.writeBuffer.size(); i++) {
			result += id / Math.pow(this.bufferSize, i);
		}
		return result;
	}

	/**
	 * Returns the map id for the given level. The map id is the number of the
	 * map entry within the given level.
	 *
	 * @param id
	 *            the block id
	 * @param level
	 *            the level
	 * @return the corresponding map id
	 */
	private long getMapID(final long id, final int level) {
		return (long) (id / Math.pow(this.bufferSize, level));
	}

	/**
	 * Translates a logical id into a physical address in the container.
	 *
	 * @param id
	 *            the logical id
	 * @return the physical address
	 */
	public TLBResult get(final long id) throws IOException {
		return get(id, this.readBuffer.size() - 1, this.readBuffer.get(this.readBuffer.size() - 1));
	}

	/**
	 * Returns the id of the last written mapping. This can be derived from the
	 * size of the TLB.
	 *
	 * @return the last successfully written mapping
	 */
	public long getLastMappingId() {
		return this.writeBuffer.get(0).mapID * this.bufferSize + this.writeBuffer.get(0).entries.size() - 1;
	}

	/**
	 * Gets the block address for the given id. Therefore, the TLB is scanned
	 * from top to bottom.
	 *
	 * @param id
	 *            the block id
	 * @param level
	 *            the level
	 * @param parentEntry
	 *            the parent entry
	 * @return the physical address of the block
	 * @throws IOException
	 */
	private TLBResult get(final long id, final int level, final TLBEntry parentEntry) throws IOException {
		final long mapID = getMapID(id, level);
		TLBEntry entry = this.readBuffer.get(level - 1);
		if ( entry.mapID != mapID ) {
			entry = this.writeBuffer.get(level - 1);
		}
		if ( entry.mapID != mapID && level == 1 && this.leafBuffer.contains(mapID) ) {
			entry = this.leafBuffer.get(mapID);
		}
		if ( entry.mapID != mapID ) {
			final int pos = (int) mapID % this.bufferSize;
			if ( pos >= parentEntry.entries.size() )
				return new TLBResult(UNMAPPED_ID_PHYSICAL, entry.nextMacroBlockID); // take last from lower level
			final long mapIdAddress = parentEntry.entries.get(pos);
			entry = deserialize(mapIdAddress, parentEntry.level - 1, pos);
			entry.mapID = mapID;
			if ( entry.level == 0 ) {
				this.leafBuffer.add(entry);
			}
			this.readBuffer.set(entry.level, entry);
		}

		if ( level == 1 ) {
			final int index = (int) (id % this.bufferSize);
			if ( index >= entry.entries.size() )
//				throw new NoSuchElementException("ID " + id + " was was never requested.");
				return new TLBResult(UNMAPPED_ID_PHYSICAL, entry.nextMacroBlockID);
			else {
				long physicalId = entry.entries.get(index);
				if ( physicalId == UNMAPPED_ID_PHYSICAL )
					return new TLBResult(UNMAPPED_ID_PHYSICAL, entry.nextMacroBlockID);
				else
					return new TLBResult(entry.entries.get(index), entry.nextMacroBlockID);
			}
		}
		else
			return get(id, level - 1, entry);
	}

	/**
	 * Looks up the leaf TLB entry containing the address for the given id. This
	 * method is required for out-of-order updates in the TLB.
	 *
	 * @param id
	 *            the id to find the containing TLB entry for.
	 * @return the TLB entry
	 */
	private Pair<Long, TLBEntry> getLeafEntry(final long id) throws IOException {
		return getTLBEntry(id, this.writeBuffer.size(), 0, this.writeBuffer.get(this.writeBuffer.size() - 1));
	}

	/**
	 * Gets the TLB entry for the given id. Therefore, the TLB is scanned from
	 * top to bottom.
	 *
	 * @param id
	 *            the block id
	 * @param level
	 *            the level
	 * @param parentEntry
	 *            the parent entry
	 * @return the physical address of the block
	 * @throws IOException
	 */
	private Pair<Long, TLBEntry> getTLBEntry(final long id, final int level, final int targetLevel,
		final TLBEntry parentEntry) throws IOException {
		final long mapID = getMapID(id, level);
		TLBEntry entry = this.readBuffer.get(level - 1);
		if ( entry.mapID != mapID ) {
			entry = this.writeBuffer.get(level - 1);
		}
		if ( entry.mapID != mapID && level == 1 && this.leafBuffer.contains(mapID) ) {
			entry = this.leafBuffer.get(mapID);
		}
		if ( entry.mapID != mapID ) {
			final int pos = (int) mapID % this.bufferSize;
			if ( pos >= parentEntry.entries.size() )
				throw new RuntimeException("Cannot read entry");
			final long mapIdAddress = parentEntry.entries.get(pos);
			entry = deserialize(mapIdAddress, parentEntry.level - 1, pos);
			entry.mapID = mapID;
			if ( entry.level == 0 ) {
				this.leafBuffer.add(entry);
			}
			this.readBuffer.set(entry.level, entry);
		}

		if ( level == targetLevel + 1 ) {
			final int pos = (int) mapID % this.bufferSize;
			final long mapIdAddress = parentEntry.entries.get(pos);
			return new Pair<>(mapIdAddress, entry);
		}
		else
			return getTLBEntry(id, level - 1, targetLevel, entry);
	}

	/**
	 * Returns the number of entries per block.
	 *
	 * @return the number of entries per block
	 */
	public long getBufferSize() {
		return this.bufferSize;
	}

	/**
	 * Deserialize a TLB entry
	 *
	 * @param mapIdAddress
	 *            the address of the tlb entry in the file
	 * @return the tlb address
	 * @throws java.io.IOException
	 */
	protected TLBEntry deserialize(final long mapIdAddress, final int level) throws IOException {
		final byte[] b = new byte[this.blockSize];
		final boolean bbOo = this.blockBuffer.isOutOfOrder();
		this.blockBuffer.setOutOfOrder(true);
		this.blockBuffer.getBlock(b, mapIdAddress / this.blockSize, false);
		this.blockBuffer.setOutOfOrder(bbOo);
		return deserialize(b);
	}

	/**
	 * Deserialize a TLB entry
	 *
	 * @param mapIdAddress
	 *            the address of the tlb entry in the file
	 * @param level
	 *            the level
	 * @param levelIndex
	 * @return the tlb address
	 * @throws java.io.IOException
	 */
	protected TLBEntry deserialize(final long mapIdAddress, final int level, final int levelIndex) throws IOException {
		final byte[] b = new byte[this.blockSize];
		this.blockBuffer.getTLBEntry(b, mapIdAddress / this.blockSize, level, levelIndex);
		return deserialize(b);
	}

	/**
	 * Deserializes the given TLB block to a TLB entry.
	 *
	 * @param b
	 *            the block to be deserialized
	 * @return the re
	 * @throws IOException
	 */
	protected TLBEntry deserialize(final byte[] b) throws IOException {
		final TLBEntry entry = new TLBEntry(this.bufferSize);
		final DataInput dataIn = new UnsafeDataInput(new ByteArrayInputStream(b), this.blockSize);
		dataIn.readShort(); // The tag
		entry.level = dataIn.readShort();
		entry.previousEntry = dataIn.readLong();
		entry.previousParentEntry = dataIn.readLong();
		entry.nextMacroBlockID = dataIn.readLong();
		final int count = dataIn.readInt();
		for (int i = 0; i < count; i++) {
			entry.entries.add(dataIn.readLong());
		}
		return entry;
	}

	/**
	 * Serialize a TLB entry
	 *
	 * @param entry
	 *            the entry
	 * @return the serialized bytes
	 * @throws java.io.IOException
	 */
	protected byte[] serialize(final TLBEntry entry, final int level) throws IOException {
		final UnsafeDataOutput dataOut = new UnsafeDataOutput(this.blockSize);
		dataOut.writeShort(TLB_TAG); // indicates a TLB entry
		dataOut.writeShort(level);
		dataOut.writeLong(entry.previousEntry);
		dataOut.writeLong(entry.previousParentEntry);
		dataOut.writeLong(entry.nextMacroBlockID);
		dataOut.writeInt(entry.entries.size());
		for (int i = 0; i < entry.entries.size(); i++) {
			dataOut.writeLong(entry.entries.get(i));
		}
		final byte[] res = dataOut.toByteArray();
		final byte[] output = new byte[this.blockSize];
		System.arraycopy(res, 0, output, 0, res.length);
		return output;
	}

	/**
	 * Record for the result of a TLB lookup.
	 */
	public static class TLBResult {

		/** The mapping id */
		long	physicalAddress;
		/** The ID of the following macro block */
		long	nextMacroBlockID;

		public TLBResult(final long physicalAddress, final long nextMacroBlockID) {
			this.physicalAddress = physicalAddress;
			this.nextMacroBlockID = nextMacroBlockID;
		}
	}

	/**
	 * @param blockBuffer
	 *            the blockBuffer to set
	 */
	public void setBlockBuffer(BlockBuffer blockBuffer) {
		this.blockBuffer = blockBuffer;
	}

}
