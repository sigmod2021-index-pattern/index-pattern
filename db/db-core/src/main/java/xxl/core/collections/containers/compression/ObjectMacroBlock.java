package xxl.core.collections.containers.compression;

import java.util.ArrayList;
import java.util.List;

/**
 * Models a macro block abstraction within the event store storage layout. This
 * implementation is based on object representations with final serialization.
 *
 */
public class ObjectMacroBlock implements MacroBlock {

	/**
	 * Defines the header size: count(byte), overflow offset(short), last block
	 * size (short)
	 */
	static final int			HEADER_SIZE				= 5;

	static final int			LENGTH_BYTES			= 2;

	/** The number of bytes required for a reference entry */
	static final int			REFERENCE_ENTRY_SIZE	= 16;

	private final int			macroBlockSize;										 // Defines the total macro block size
	private int					offset;														 // Defines the offset size
	private int					length;														 // Defines the payload size
	private final float			spare;														 // Defines the spare size (no spare for 1.0)
	private final List<Integer>	sizes;														 // Stores the (logical) sizes of all blocks
	private final List<byte[]>	blocks;														 // Stores the blocks
	private byte[]				blockOffset;										 // Stores the offset

	/**
	 * Creates a new dense macro block (without spare).
	 *
	 * @param macroBlockSize
	 *            the maximum total size of the macro block (in bytes)
	 */
	public ObjectMacroBlock(final int macroBlockSize) {
		this(macroBlockSize, 0.0f);
	}

	/**
	 * Creates a macro block with a predefined spare.
	 *
	 * @param macroBlockSize
	 *            The maximum total size of the macro block (in bytes)
	 * @param spare
	 *            the maximum fill rate of the macro block. Therefore, the spare
	 *            ist defined as macro block size * (spare)
	 */
	public ObjectMacroBlock(final int macroBlockSize, final float spare) {
		this.macroBlockSize = macroBlockSize;
		this.spare = spare;
		this.blocks = new ArrayList<>();
		this.sizes = new ArrayList<>();
	}

	/**
	 * Loads the given macro block.
	 *
	 * @param b
	 *            the binary representation of the macro block
	 */
	public ObjectMacroBlock(final byte[] b) {
		this(b.length);
		loadMacroBlock(b);
	}

	/**
	 * Adds a block to this macro block.
	 *
	 * @param block
	 *            the block
	 * @param blockSize
	 *            the total size of the block. This is important if the block
	 *            does not fit into this macro block. In that case, block only
	 *            contains the part for this macro block
	 */
	@Override
	public synchronized void add(final byte[] block, final int blockSize) {
		if ( freeSpace() < block.length )
			throw new RuntimeException("There is not enough space for insertion");
		if ( block.length > blockSize )
			throw new RuntimeException("Actual block size is greater than the declared block size");
		if ( block.length < blockSize && freeSpace() > blockSize )
			throw new RuntimeException("Actual block size is smaller than the declared block size, "
					+ "though there would be enough space in the block");
		this.length += block.length + 2; // The block as well as the size entry have to be considered
		this.sizes.add(blockSize);
		this.blocks.add(block);
	}

	/**
	 * Returns the entry at the given position within this macro block.
	 *
	 * @param id
	 *            the id (position) of the entry
	 * @return the bytes of the requested entry within this block and its total
	 *         size
	 */
	@Override
	public EntryResult getEntry(final int id) {
		if ( id < 0 || id >= this.sizes.size() )
			throw new RuntimeException("Invalid entry " + id + "!");
		return new EntryResult(this.blocks.get(id), this.sizes.get(id));
	}

	/**
	 * Adds the remainder of the last block of the previous macro block to this
	 * one.
	 *
	 * @param blockOffset
	 *            the remainder of the last block
	 */
	@Override
	public void addOffset(final byte[] blockOffset) {
		this.blockOffset = blockOffset;
		this.offset = blockOffset.length;
	}

	/**
	 * Returns the offset of this macro block.
	 *
	 * @return the offset bytes of the previous block
	 */
	@Override
	public byte[] getOffset() {
		if ( this.blockOffset == null )
			return new byte[0];
		else
			return this.blockOffset;
	}

	/**
	 * Updates the block with the given id.
	 *
	 * @param id
	 *            the id of the block to be updated
	 * @param block
	 *            the block
	 * @param blockSize
	 *            the full size of the block
	 */
	@Override
	public void replace(final int id, final byte[] block, final int blockSize) {
		final int resize = block.length - this.blocks.get(id).length;
		if ( resize <= updateSpace() ) {
			this.blocks.set(id, block);
			this.sizes.set(id, blockSize);
			this.length += resize;
		}
		else
			throw new RuntimeException("Block exceeds the maximum spare in the block!");
	}

	/**
	 * Updates the offset of the current block.
	 *
	 * @param b
	 *            the new offset
	 */
	@Override
	public void updateOverflow(final byte[] b) {
		if ( b.length - this.offset > updateSpace() )
			throw new RuntimeException("There is not enough space to update the offset!");
		this.blockOffset = b;
		this.offset = b.length;
	}

	@Override
	public int getCount() {
		return this.blocks.size();
	}

	@Override
	public int getLength() {
		return HEADER_SIZE + this.offset + this.length;
	}

	/**
	 * Returns if the block at the given position is an overflow block
	 *
	 * @param id
	 *            the number of the block
	 * @return true, if the is an overflow block, false otherwise
	 */
	@Override
	public boolean isOverflowBlock(final int id) {
		if ( id < 0 || id >= this.sizes.size() )
			throw new RuntimeException("Invalid entry " + id + "!");
		return this.sizes.get(id) > this.blocks.get(id).length;
	}

	/**
	 * Sets the given entry as last entry. I.e., removes all entries after the
	 * given index! If id == -1, all entries are removed from the block. This is
	 * required for blocks containing overflows.
	 *
	 * @param id
	 *            the id of the block to be set as last block
	 */
	@Override
	public synchronized void setToLast(final int id) {
		if ( id == -1 ) { //
			this.blocks.clear();
			this.sizes.clear();
			this.length = 0;
		}
		else {
			while (this.blocks.size() - 1 > id) {
				this.length -= this.blocks.get(id + 1).length;
				this.blocks.remove(id + 1);
			}
			while (this.sizes.size() - 1 > id) {
				this.length -= 2;
				this.sizes.remove(id + 1);
			}
		}
	}

	/**
	 * Returns the free available space in this block. The amount of free space
	 * relates to the actual payload. Additional space for meta information is
	 * already considered. Spare is not considered as &quot;free&quot;.
	 *
	 * @return the free space in bytes
	 */
	@Override
	public int freeSpace() {
		// The available free space (2 length bytes required for new block)
		final int size = this.macroBlockSize - ((int) (this.macroBlockSize * this.spare)) - HEADER_SIZE - this.offset
				- this.length - LENGTH_BYTES;
		return (size > 0) ? size : 0; // short value for each length entry (allows 64K entry size)
	}

	/**
	 * Returns the total amount of space available in the macro block. This
	 * includes the update space as well as the insert space!
	 *
	 * @return the free available space in bytes
	 */
	@Override
	public int updateSpace() {
		final int size = this.macroBlockSize - HEADER_SIZE - this.offset - this.length; // The available free space
		return (size > 0) ? size : 0; // short value for each length entry (allows 64K entry size)
	}

	// =================================================================================================================

	/**
	 * De-serializes the given block.
	 *
	 * @param b
	 *            the block to be deserialize
	 */
	public void loadMacroBlock(final byte[] b) {
		int pos = 0;
		final byte bcount = b[pos++];
		int count;
		if ( bcount < 0 ) {
			count = unsignedToBytes(bcount);
		}
		else {
			count = bcount;
		}

		this.offset = readLength(b, pos);
		pos += 2;
		final int lastBlockSize = readLength(b, pos);
		pos += 2;
		if ( this.offset > 0 ) {
			this.blockOffset = readBlock(b, pos, this.offset);
		}
		pos += this.offset;
		for (int i = 0; i < count; i++) {
			final int size = readLength(b, pos);
			pos += 2;
			byte[] block;
			if ( size == -1 ) {
				block = readBlock(b, pos, REFERENCE_ENTRY_SIZE);
			}
			else if ( size >= 0 ) {
				block = readBlock(b, pos, (i == count - 1) ? lastBlockSize : size);
			}
			else
				throw new RuntimeException("Negative array size!");
			pos += block.length;
			this.sizes.add(size);
			this.blocks.add(block);
		}
		this.length = pos - HEADER_SIZE - this.offset;
	}

	private static int unsignedToBytes(final byte b) {
		return b & 0xFF;
	}

	/**
	 * Serializes this macro block.
	 *
	 * @return the serialized macro block representation.
	 */
	@Override
	public void getBytes(final byte[] b) {
		// update count value in byte array
		int pos = 0;
		// Write number of entries
		if ( this.blocks.size() > 255 )
			throw new RuntimeException(
					"Error writing macro block: number of blocks (" + this.blocks.size() + ") exceeds maximum (255)!");
		b[pos++] = (byte) this.blocks.size();
		// Write offset (if existing)
		pos = setLength(b, pos, this.offset);
		// Write size of last block
		pos = setLength(b, pos, this.blocks.size() > 0 ? this.blocks.get(this.blocks.size() - 1).length : 0);
		if ( this.blockOffset != null ) {
			pos = appendBlock(b, pos, this.blockOffset);
		}
		// Write payload
		for (int i = 0; i < this.blocks.size(); i++) {
			pos = setLength(b, pos, this.sizes.get(i));
			pos = appendBlock(b, pos, this.blocks.get(i));
		}
	}

	// =================================================================================================================

	/**
	 * Returns if the entry at the given position is a reference entry.
	 *
	 * @param id
	 *            the id of the requested entry
	 * @return true, if the entry is a reference entry, false otherwise
	 */
	@Override
	public boolean isReferenceEntry(final int id) {
		return this.sizes.get(id) == -1;
	}

	@Override
	public ReferenceEntry readReferenceEntry(final int id) {
		final long reference = readReference(this.blocks.get(id), 0);
		final long previousWrittenId = readReference(this.blocks.get(id), 8);
		return new ReferenceEntry(reference, previousWrittenId);
	}

	@Override
	public void setReferenceEntry(final int id, final ReferenceEntry reference) {
		final byte[] ref = new byte[16];
		writeReference(ref, 0, reference.getPhysicalAddress());
		writeReference(ref, 8, reference.getPreviousWrittenId());
		this.length -= this.blocks.get(id).length;
		this.blocks.set(id, ref);
		this.sizes.set(id, -1);
		this.length += ref.length;
	}

	// =================================================================================================================

	/**
	 * Appends the given block to the given array at the given position and
	 * updates the position.
	 *
	 * @param b
	 *            the byte array to append to
	 * @param pos
	 *            the position to append at
	 * @param block
	 *            the block to be appended
	 * @return the new position after appending the block
	 */
	private int appendBlock(final byte[] b, int pos, final byte[] block) {
		System.arraycopy(block, 0, b, pos, block.length);
		pos += block.length;
		return pos;
	}

	/**
	 * Reads out the block with the given size at the given position of the
	 * given byte array.
	 *
	 * @param b
	 *            the byte array
	 * @param pos
	 *            the position of the block
	 * @param size
	 *            the size of the block
	 * @return the block
	 */
	private byte[] readBlock(final byte[] b, final int pos, final int size) {
		final byte[] result = new byte[size];
		System.arraycopy(b, pos, result, 0, size);
		return result;
	}

	/**
	 * Sets the length at the given position and updates the position.
	 *
	 * @param pos
	 *            the position of the length entry
	 * @param length
	 *            the new length
	 * @return the new position after writing the length
	 */
	public int setLength(final byte[] b, int pos, final int length) {
		final short l = (short) length;
		// update length information for block
		b[pos++] = (byte) (l & 0xFF);
		b[pos] = (byte) ((l >> 8) & 0xFF);
		return pos + 1;
	}

	/**
	 * Writes a reference entry.
	 *
	 * @param b
	 *            the byte array to write into
	 * @param pos
	 *            the position within the array
	 * @param l
	 *            the reference entry
	 */
	private void writeReference(final byte[] b, final int pos, long l) {
		for (int i = 7; i >= 0; i--) {
			b[pos + i] = (byte) (l & 0xFF);
			l >>= 8;
		}
	}

	/**
	 * Reads a reference entry.
	 *
	 * @param b
	 *            the byte array to read from
	 * @param pos
	 *            the position within the array
	 * @return the reference entry
	 */
	private long readReference(final byte[] b, final int pos) {
		long result = 0;
		for (int i = 0; i < 8; i++) {
			result <<= 8;
			result |= (b[pos + i] & 0xFF);
		}
		return result;
	}

	/**
	 * Reads a length entry (short value) from the binary representation.
	 *
	 * @param pos
	 *            the position of the length entry
	 * @return the length
	 */
	private int readLength(final byte[] b, final int pos) {
		final short l = (short) ((b[pos + 1] << 8) | (0xFF & b[pos]));
		return l;
	}

	// =================================================================================================================
}
