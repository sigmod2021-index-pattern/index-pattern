package xxl.core.collections.containers.compression;

import java.io.ByteArrayInputStream;
import java.io.DataInput;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.concurrent.locks.ReentrantLock;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import sigmod2021.db.DBRuntimeException;
import xxl.core.collections.containers.AbstractContainer;
import xxl.core.collections.containers.compression.TLB.TLBResult;
import xxl.core.collections.containers.compression.buffer.BlockBuffer;
import xxl.core.collections.containers.compression.buffer.impl.BasicBlockBuffer;
import xxl.core.collections.containers.compression.buffer.impl.NoBlockBuffer;
import xxl.core.collections.containers.compression.rawAccess.FileChannelRawAccess;
import xxl.core.collections.containers.io.SuspendableContainer;
import xxl.core.io.Block;
import xxl.core.io.Buffer;
import xxl.core.io.LRUBuffer;
import xxl.core.io.UnsafeDataInput;
import xxl.core.io.UnsafeDataOutput;
import xxl.core.io.converters.FixedSizeConverter;
import xxl.core.io.converters.LongConverter;
import xxl.core.io.raw.RawAccess;
import xxl.core.util.WrappingRuntimeException;

/**
 * Container for variable size, compressed blocks. Blocks are encapsulated in
 * macro blocks. Logical IDs are mapped to physical addresses via a TLB. This
 * allows full sequential writing and reading performance. Random access to a
 * block requires 2 block accesses in the worst case.
 *
 */
public class FastCompressionContainer extends AbstractContainer implements SuspendableContainer {

	/** The container states */
	protected static final int					CONTAINER_OPEN				= 0;
	protected static final int					CONTAINER_CLOSED			= 1;
	protected static final int					CONTAINER_REOPENED			= 2;
	//    protected static final int CONTAINER_RECOVERED = 3;
	
	/**
	 * The logger
	 */
	private final Logger					logger						= LoggerFactory
		.getLogger(FastCompressionContainer.class);
	
	/**
	 * The number of blocks used for the (sequential read) block buffer in order
	 * to compensate the overhead created by the container.
	 */
	protected static final int					CONTAINER_OVERHEAD_BLOCKS	= 300;
	/**
	 * The size of a reference entry
	 */
	protected static final int					REFERENCE_ENTRY_SIZE		= 16;
	
	////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

	/** The container path. */
	private final String						path;
	/** Lock for synchronizing accesses to the container */
	final ReentrantLock							lock;
	/** The size of a disk block */
	final int									blockSize;
	/** The size of a macro block */
	private final int							macroBlockSize;
	/**
	 * The amount of space that should be kept for out-of-order updates. Spare
	 * is only set for fresh created MacroBlocks, if loaded it is zero --> when
	 * writing sequentially, spare space is left, on updates, blocks may expand
	 * to full size
	 */
	private final float							spare;
	/** The size of the preamble, persisting the state of the container */
	private final int							preambleSize;
	/** The compressor for data compression. */
	final Compressor							compressor;

	/** Write buffer for blocks. */
	private final byte[]						outputBlock;
	/** Write buffer for macro blocks. */
	private final byte[]						macroOutputBlock;

	/** The read buffer for sequential reading */
	final BlockBuffer							blockBuffer;
	/** A buffer for macro blocks (required for out-of-order) */
	private final Buffer<Object, Long, byte[]>	macroBlockBuffer;
	
	/** The file handle for the data on disk. */
	private RawAccess							raf;
	/** The TLB for address translation logical ID => physical position */
	TLB											tlb;

	/** The state of the container (opened, reopened, closed) */
	private int									state;
	/** Counter for logical page IDs */
	private long								idCounter;
	/** Counter for macro blocks */
	long										physicalBlockIdCounter;
	/** The current physical position to write to */
	long										physicalPosition;
	/** The position of the last container read. */
	private final long							lastContainerPosition		= 0;
	/** The current macro block to be filled. */
	MacroBlock									currentMacroBlock;
	/** The last loaded macro block id */
	private long								lastLoadedMacroBlockId		= -1;
	/** The last loaded macro block */
	private byte[]								lastLoadedMacroBlock;

	/** Stores if the container is suspended */
	private boolean								suspended;

	/**
	 * Creates a new fast compression container with macroBlockSize =
	 * 4*blockSize.
	 *
	 * @param path
	 *            the path of the container
	 * @param blockSize
	 *            the size of a block
	 */
	public FastCompressionContainer(final String path, final int blockSize, final float spare, int macroBlockBufferSize, boolean useDirectIO, boolean useBlockBuffer) {
		this(path, 4 * blockSize, blockSize, spare, macroBlockBufferSize, useDirectIO, useBlockBuffer);
	}

	/**
	 * Creates a new fast compression container.
	 *
	 * @param path
	 *            the path of the container
	 * @param macroBlockSize
	 *            the size of a macro block
	 * @param blockSize
	 *            the size of a block
	 */
	public FastCompressionContainer(final String path, final int macroBlockSize, final int blockSize,
		final float spare, int macroBlockBufferSize, boolean useDirectIO, boolean useBlockBuffer) {
		this(path, macroBlockSize, blockSize, new LZ4Compressor(), spare, macroBlockBufferSize,useDirectIO,useBlockBuffer);
	}

	/**
	 * Creates a new fast compression container.
	 *
	 * @param path
	 *            the path of the container
	 * @param macroBlockSize
	 *            the size of a macro block
	 * @param blockSize
	 *            the size of a block
	 */
	public FastCompressionContainer(final String path, final int macroBlockSize, final int blockSize,
		final Compressor compressor, final float spare, int macroBlockBufferSize, boolean useDirectIO, boolean useBlockBuffer) {
		if ( macroBlockSize < blockSize )
			throw new RuntimeException("Macro block size has to be always a multiple of the blockSize!");
		this.path = path;
		this.macroBlockBuffer = new LRUBuffer<>(macroBlockBufferSize);
		this.macroBlockSize = macroBlockSize;
		this.blockSize = blockSize;
		this.preambleSize = calculatePreambleSize();
		this.physicalPosition = this.preambleSize;
		this.compressor = compressor;
		this.spare = spare;
		try {
			if ( !(new File(path + ".ctr")).exists() ) {
				new File(path + ".ctr").createNewFile();
			}
			this.raf = new FileChannelRawAccess(this.blockSize, useDirectIO);
			this.raf.open(this.path + ".ctr");
			// reserve and initialize preamble
			initPreamble();
		}
		catch (final FileNotFoundException e) {
			throw new RuntimeException("Error creating container at path " + path,e);
		}
		catch (final IOException e) {
			throw new RuntimeException("Error writing preamble " + path,e);
		}
		this.tlb = new TLB(this.raf, blockSize);
		this.currentMacroBlock = createMacroBlock(macroBlockSize);
		this.lock = new ReentrantLock(true);
		this.state = CONTAINER_OPEN;
		this.outputBlock = new byte[blockSize];
		this.macroOutputBlock = new byte[macroBlockSize];

		this.blockBuffer = createBlockBuffer(this.raf,useBlockBuffer);
		this.tlb.setBlockBuffer(blockBuffer);
	}

	/**
	 * Loads a fast compression container.
	 *
	 * @param path
	 *            the path of the container
	 */
	public FastCompressionContainer(final String path, int macroBlockBufferSize, boolean useDirectIO, boolean useBlockBuffer) {
		this.path = path;
		this.lock = new ReentrantLock(true);
		try {
			this.macroBlockBuffer = new LRUBuffer<>(macroBlockBufferSize);
			
			//Checking block size
			final FileInputStream fis = new FileInputStream(path + ".ctr");
			DataInput dis = new UnsafeDataInput(fis);
			this.state = dis.readByte();
			this.blockSize = dis.readInt();
			fis.close();

			this.raf = new FileChannelRawAccess(this.blockSize, useDirectIO);
			this.raf.open(this.path + ".ctr");
			// Handling meta data in own block
			final byte[] dat = new byte[this.blockSize];
			this.raf.read(dat, 0);
			dis = new UnsafeDataInput(new ByteArrayInputStream(dat));
			// skip state and block-size
			dis.readByte();
			dis.readInt();
			//            dis.skipBytes(5);
			this.macroBlockSize = dis.readInt();
			this.spare = dis.readFloat();
			final String compressorClass = dis.readUTF();
			try {
				this.compressor = (Compressor) Class.forName(compressorClass).newInstance();
				this.compressor.restoreParams(dis);
			}
			catch (InstantiationException | IllegalAccessException | ClassNotFoundException ex) {
				throw new IllegalStateException(String.format("Compressor (%s) could not be created.", compressorClass),
					ex);
			}

			this.tlb = new TLB(this.raf, this.blockSize);
			this.blockBuffer = createBlockBuffer(this.raf,useBlockBuffer);
			this.tlb.setBlockBuffer(blockBuffer);

			this.preambleSize = calculatePreambleSize();
			this.outputBlock = new byte[this.blockSize]; // The block for writing out macro blocks
			this.macroOutputBlock = new byte[this.macroBlockSize];
			loadState(dis);
		}
		catch (final FileNotFoundException e) {
			throw new DBRuntimeException("Error loading container at path " + path, e);
		}
		catch (final IOException e) {
			throw new DBRuntimeException("Error loading container state at path " + path, e);
		}
	}

	/**
	 * Creates a new empty macro block with the given size.
	 *
	 * @param size
	 *            the size of the macro block.
	 * @return a new, empty macro block
	 */
	private MacroBlock createMacroBlock(final int size) {
		return new ObjectMacroBlock(size, this.spare);
	}

	/**
	 * Loads a macro block from the given binary representation.
	 *
	 * @param b
	 *            the binary representation of the macro block
	 * @return the loaded macro block
	 */
	private MacroBlock createMacroBlock(final byte[] b) {
		return new ObjectMacroBlock(b);
	}

	/**
	 * Calculates the size of the preamble, containing all meta-information for
	 * this container.
	 *
	 * @return the size of the preamble
	 */
	private int calculatePreambleSize() {
		return (6 + this.macroBlockSize / this.blockSize) * this.blockSize;
	}

	/**
	 * Reads the preamble and restores the state.
	 *
	 * @throws java.io.IOException
	 */
	private void loadState(final DataInput dis) throws IOException {
		if ( this.state == CONTAINER_OPEN || this.state == CONTAINER_REOPENED ) { // The container was not written finally
			recover(); // recover the container
		}
		else { // Load remainder of the state
			final byte tlbCount = dis.readByte();
			final long[] tlbMapIDs = new long[tlbCount];
			for (int i = 0; i < tlbCount; i++) {
				tlbMapIDs[i] = dis.readLong();
			}
			this.idCounter = dis.readLong();
			this.physicalBlockIdCounter = dis.readLong();
			this.physicalPosition = dis.readLong();
			// Read macro block.
			final byte[] m = loadMacroBlock(1);
			// Read TLB
			this.tlb.writeBuffer.clear();
			this.tlb.readBuffer.clear();
			for (int i = 0; i < tlbCount; i++) {
				// 1 Block Meta, 4 Blocks current macro block = 5
				final TLB.TLBEntry tlbEntry = this.tlb.deserialize((5 + i) * this.blockSize, 1);
				tlbEntry.mapID = tlbMapIDs[i];
				this.tlb.writeBuffer.add(i, tlbEntry);
				this.tlb.readBuffer.add(i, tlbEntry); // init read buffer
			}
			this.currentMacroBlock = createMacroBlock(m); // For binary representation, also the length of the macro block has to be set!
			finishPreamble(CONTAINER_REOPENED);
			this.state = CONTAINER_REOPENED;
		}

	}

	/**
	 * Writes the settings (block size and macro block size) to the preamble. In
	 * total, 9 bytes are written.
	 *
	 * @throws java.io.IOException
	 */
	private void initPreamble() throws IOException {
		final UnsafeDataOutput dos = new UnsafeDataOutput(this.blockSize);

		dos.writeByte(CONTAINER_OPEN);  // Indicates that the container has not been closed yet (required for recovery)
		dos.writeInt(this.blockSize);
		dos.writeInt(this.macroBlockSize);
		dos.writeFloat(this.spare);
		dos.writeUTF(this.compressor.getClass().getCanonicalName());
		this.compressor.writeParams(dos);

		final byte[] preamble = new byte[this.blockSize];
		final byte[] written = dos.toByteArray();
		System.arraycopy(written, 0, preamble, 0, written.length);
		this.raf.write(preamble, 0);
	}

	/**
	 * Finishes the preamble.
	 *
	 * @throws java.io.IOException
	 */
	private void finishPreamble(final int containerState) throws IOException {
		final UnsafeDataOutput dos = new UnsafeDataOutput(this.blockSize);
		dos.writeByte(containerState);
		dos.writeInt(this.blockSize);
		dos.writeInt(this.macroBlockSize);
		dos.writeFloat(this.spare);
		dos.writeUTF(this.compressor.getClass().getCanonicalName());
		this.compressor.writeParams(dos);

		dos.writeByte(this.tlb.writeBuffer.size());
		for (int i = 0; i < this.tlb.writeBuffer.size(); i++) {
			dos.writeLong(this.tlb.writeBuffer.get(i).mapID);
		}
		dos.writeLong(this.idCounter);
		dos.writeLong(this.physicalBlockIdCounter);
		dos.writeLong(this.physicalPosition);
		final byte[] preamble = new byte[this.blockSize];
		final byte[] written = dos.toByteArray();
		System.arraycopy(written, 0, preamble, 0, written.length);
		// Write meta data of preamble
		this.raf.write(preamble, 0);
	}

	/**
	 * Preserves the final state of the container
	 *
	 * @throws IOException
	 */
	private void writeStates() throws IOException {
		//        // Write current macro block
		final byte[] mbout = new byte[this.macroBlockSize];
		this.currentMacroBlock.getBytes(mbout);
		writeToDisk(mbout, this.physicalBlockIdCounter, 1, false);

		// Old: Was possibly stuck in the buffer
		//      writeMacroBlock(currentMacroBlock, physicalBlockIdCounter, 1);

		// Write TLB blocks
		for (int i = 0; i < this.tlb.writeBuffer.size(); i++) {
			this.raf.write(this.tlb.serialize(this.tlb.writeBuffer.get(i), i), 5 + i);
		}
	}

	/**
	 * Prints the state of this container to the log.
	 */
	public String logState() {
		final StringBuilder sb = new StringBuilder();
		sb.append("=============================");
		sb.append("\r\n");
		sb.append("Variables:");
		sb.append("\r\n");
		sb.append("-----------------------------");
		sb.append("\r\n");
		// Variables
		sb.append("idCounter: " + this.idCounter);
		sb.append("\r\n");
		sb.append("physicalBlockIdCounter: " + this.physicalBlockIdCounter);
		sb.append("\r\n");
		sb.append("lastContainerPosition: " + this.lastContainerPosition);
		sb.append("\r\n");
		sb.append("lastLoadedMacroBlockId: " + this.lastLoadedMacroBlockId);
		sb.append("\r\n");
		sb.append("physicalPosition: " + this.physicalPosition);
		sb.append("\r\n");
		sb.append("state: " + this.state);
		sb.append("\r\n");
		sb.append("spare: " + this.spare);
		sb.append("\r\n");
		sb.append("suspended: " + this.suspended);
		sb.append("\r\n");
		// TLB
		sb.append("-----------------------------");
		sb.append("\r\n");
		sb.append("TLB:");
		sb.append("\r\n");
		sb.append("-----------------------------");
		sb.append("\r\n");
		sb.append("Last Mapping: " + this.tlb.getLastMappingId());
		sb.append("\r\n");
		sb.append("Buffer size: " + this.tlb.getBufferSize());
		sb.append("\r\n");
		if ( this.tlb.writeBuffer.get(0).entries.size() > 0 ) {
			sb.append("First TLB[0] entry:" + this.tlb.writeBuffer.get(0).entries.get(0));
			sb.append("\r\n");
			sb.append("Last TLB[0] entry:" +
				this.tlb.writeBuffer.get(0).entries.get(this.tlb.writeBuffer.get(0).entries.size() - 1));
			sb.append("\r\n");
		}
		// Buffer
		sb.append("-----------------------------");
		sb.append("\r\n");
		sb.append(this.blockBuffer.logState());
		// Current Macro block
		sb.append("-----------------------------");
		sb.append("\r\n");
		sb.append("currentMacroBlock:");
		sb.append("\r\n");
		sb.append("-----------------------------");
		sb.append("\r\n");
		final MacroBlock omb = this.currentMacroBlock;
		sb.append("count: " + omb.getCount());
		sb.append("\r\n");
		sb.append("offset: " + omb.getOffset().length);
		sb.append("\r\n");
		sb.append("length: " + omb.getLength());
		sb.append("\r\n");
		sb.append("free space: " + omb.freeSpace());
		sb.append("\r\n");
		sb.append("update space: " + omb.updateSpace());
		sb.append("\r\n");
		sb.append("Entries:");
		sb.append("\r\n");
		for (int i = 0; i < omb.getCount(); i++) {
			sb.append("  " + i + ": [" + omb.getEntry(i).array.length + "] : " + omb.getEntry(i).length + "\r\n");
		}
		sb.append("\r\n");
		sb.append("=============================");
		return sb.toString();
	}

	@Override
	public void flush() {
		super.flush();
		this.macroBlockBuffer.flushAll(this);
	}

	@Override
	public void close() {
		super.close();
		try {
			this.lock.lock();
			// Write container state, TLB and current MacroBlock
			this.macroBlockBuffer.flushAll(this);
			finishPreamble(CONTAINER_CLOSED);
			writeStates();
			this.raf.close();
		}
		catch (final IOException e) {
			e.printStackTrace();
		}
		finally {
			this.lock.unlock();
		}
	}

	/**
	 * Creates new raw access.
	 *
	 * @return the new raw access
	 */
	private BlockBuffer createBlockBuffer(RawAccess ra, boolean useBlockBuffer) {
		if ( !useBlockBuffer )
			return new NoBlockBuffer(ra);
		else
			return new BasicBlockBuffer((int) this.tlb.getBufferSize() + CONTAINER_OVERHEAD_BLOCKS,
				(int) this.tlb.getBufferSize(), this.blockSize, ra);
	}

	/**
	 * Returns the container position based on id and macro block id. This
	 * method calculates the number of TLB blocks before the current block and,
	 * based on that, the position of the macro block within the container.
	 *
	 * @param lid
	 *            the id of the block
	 * @param macroBlockID
	 *            the if of the macro block
	 * @return the position of the macro block within the container
	 * @throws java.io.IOException
	 */
	private long getContainerPosition(final long lid, final long macroBlockID) throws IOException {

		final TLB.TLBResult lookupResult = this.tlb.get(lid);
		logger.trace("Next macro block ID in TLB entry for lid " + lid + " in macro block " + macroBlockID + ": " +
			lookupResult.nextMacroBlockID);
		if ( lookupResult.physicalAddress != -1 && macroBlockID == lookupResult.nextMacroBlockID ) {
			final long nextSwitchTLB = (lid / this.tlb.getBufferSize() + 1L) * this.tlb.getBufferSize(); // the next TLB after causing an overflow
			return getMacroBlockPosition(nextSwitchTLB, macroBlockID);
		}
		else
			return getMacroBlockPosition(lid, macroBlockID);
	}

	@Override
	public synchronized Object get(final Object id, final boolean unfix) throws NoSuchElementException {
		final long lid = (Long) id;
		try {
			return loadBlock(lid);
		}
		catch (final Exception e) {
			throw new RuntimeException("Error retrieving ID " + id + ": " + e.getMessage(),e);
		}
	}

	private Object loadBlock(final long lid) throws Exception {
		final MacroBlockResult mbr = getMacroBlockForID(lid);
		MacroBlockResult overflowMB = null;
		final int entryNumber = mbr.entryNumber;
		MacroBlock b = mbr.mb;

		// The entry is not a reference entry
		final EntryResult loopUpResult = b.getEntry(entryNumber);
		if ( loopUpResult.array.length < loopUpResult.length ) {
			// read next logical node and extract offset
			final int missingBytes = loopUpResult.length - loopUpResult.array.length;
			// get next container`s offset
			overflowMB = getNextMacroBlock(mbr.macroBlockID, mbr.containerPosition);
			b = overflowMB.mb;
			final byte[] missing = b.getOffset();
			if ( missing.length != missingBytes )
				throw new RuntimeException("Error reading data for id " + lid);
			final byte[] newResult = new byte[loopUpResult.length];
			System.arraycopy(loopUpResult.array, 0, newResult, 0, loopUpResult.array.length);
			System.arraycopy(missing, 0, newResult, loopUpResult.array.length, missing.length);
			loopUpResult.array = newResult;
		}
		if ( loopUpResult.array.length == this.blockSize )
			return new Block(loopUpResult.array);
		else
			return new Block(this.compressor.decompress(loopUpResult.array, this.blockSize));
	}

	/**
	 * Returns the next macro block for the given one.
	 *
	 * @param lid
	 *            the macro block id of the current block
	 * @param containerPosition
	 *            the position of the current block
	 * @return the next macro block result
	 */
	private MacroBlockResult getNextMacroBlock(final long lid, final long containerPosition) {
		if ( lid == this.physicalBlockIdCounter - 1 )
			return new MacroBlockResult(lid + 1, -1, this.physicalPosition, this.currentMacroBlock);
		long block = (containerPosition + this.macroBlockSize) / this.blockSize; // the block to read
		final byte[] b = new byte[this.blockSize];
		try {
			this.lock.lock();
			while (block < this.raf.getNumSectors()) {
				if ( this.macroBlockBuffer.contains(this, block) ) {
					// Macro block is in the buffer --> Load macro block directly
					final byte[] result = loadMacroBlock(block);
					return new MacroBlockResult(lid + 1, -1, block * this.blockSize, createMacroBlock(result));
				}
				final boolean bbOo = this.blockBuffer.isOutOfOrder();
				
				
				this.blockBuffer.setOutOfOrder(true);
				this.blockBuffer.getBlock(b, block, false);
				this.blockBuffer.setOutOfOrder(bbOo);
				if ( !isTLB(b) ) {
					// Load macro block
					final byte[] result = loadMacroBlock(block);
					return new MacroBlockResult(lid + 1, -1, block * this.blockSize, createMacroBlock(result));
				}
				block += 1;
			}
			return new MacroBlockResult(lid + 1, -1, this.physicalPosition, this.currentMacroBlock);
		}
		catch (final IOException ex) {
			throw new RuntimeException("Error while reading next macro block");
		}
		finally {
			this.lock.unlock();
		}
	}

	/**
	 * Returns if the given block is a TLB block.
	 *
	 * @param b
	 * @return
	 * @throws IOException
	 */
	private boolean isTLB(final byte[] b) throws IOException {
		final DataInput dataIn = new UnsafeDataInput(new ByteArrayInputStream(b));
		final short tag = dataIn.readShort();
		return tag == TLB.TLB_TAG;
	}

	@Override
	public Iterator<Long> ids() {
		return new Iterator<Long>() {
			long	pos	= 0;
			final long	max	= FastCompressionContainer.this.idCounter;
			Long next = computeNext();
			
			private Long computeNext() {
				Long result = null;
				for (; pos < max && result == null; pos++ ) {
					if ( isUsed(pos) )
						result = pos;
				}
				return result;
			}
			

			@Override
			public boolean hasNext() {
				return next != null;
			}

			@Override
			public Long next() {
				Long result = next;
				next = computeNext();
				return result;
			}

			@Override
			public void remove() {
				throw new UnsupportedOperationException();
			}
		};
	}

	@Override
	public boolean contains(final Object id) {
		return isUsed(id);
	}

	@Override
	public boolean isUsed(final Object id) {
		long lid = (Long) id;
		if ( lid < 0 || lid >= this.idCounter ) 
			return false;
		
		try {
			TLBResult r = tlb.get(lid);
			if ( r.physicalAddress == TLB.UNMAPPED_ID_PHYSICAL ) {
				return false;
			}
			else {
				return true;
			}
		} catch ( IOException e ) {
			return false;
		}
	}

	@Override
	public FixedSizeConverter<Long> objectIdConverter() {
		return LongConverter.DEFAULT_INSTANCE;
	}

	@Override
	public void remove(final Object id) throws NoSuchElementException {
		logger.trace("Removing id " + id);

		final long lid = (Long) id;
		//Semantics: sets the predecessor as last entry
		if ( this.idCounter == lid + 1 ) {
			this.idCounter = lid;
			return;
		}

		try {
			this.lock.lock();
			final long physicalAddress = this.tlb.get(lid).physicalAddress;
			final int entryNumber = decodeEntryNumber(physicalAddress);
			final long macroBlockID = decodeBlockID(physicalAddress);

			long containerPosition = getContainerPosition(lid, macroBlockID);
			MacroBlock mb = (containerPosition < this.physicalPosition) ? createMacroBlock(
				loadMacroBlock(containerPosition / this.blockSize)) : this.currentMacroBlock;

			if ( entryNumber == 0 ) {
				if ( mb.getOffset().length > 0 ) {
					// mb should be current macro block
					mb.setToLast(entryNumber - 1);
					this.physicalBlockIdCounter = macroBlockID;
				}
				// No overflow, but not first MB
				else if ( macroBlockID > 0 ) {
					// the predecessor of mb should be current macro block
					containerPosition = getContainerPosition(lid - 1, macroBlockID - 1);
					mb = createMacroBlock(loadMacroBlock(containerPosition / this.blockSize));
					this.physicalBlockIdCounter = macroBlockID - 1;
				}
				// First block cleaned
				else {
					this.physicalBlockIdCounter = 0;
					// Clean block
					mb.setToLast(-1);
				}
			}
			else {
				mb.setToLast(entryNumber - 1);
				physicalBlockIdCounter = macroBlockID;
			}

			// TODO: Shrink... in case of crash, recovery will recover deleted blocks
			if ( mb != this.currentMacroBlock ) {
				final long cp = getContainerPosition(Math.max(0, lid - 1), macroBlockID);
				this.physicalPosition = cp;
			}

			this.currentMacroBlock = mb;
			final int toRemove = (int) (this.idCounter - lid);
			for (int i = 0; i < toRemove; i++) {
				this.tlb.removeLastMapping();
			}
			this.idCounter = lid;
		}
		catch (final IOException ex) {
			throw new NoSuchElementException("Error removing ID " + id);
		}
		finally {
			this.lock.unlock();
		}
	}

	@SuppressWarnings({ "rawtypes", "deprecation" })
	@Override
	public Object reserve(final xxl.core.functions.Function getObject) {
		return this.idCounter++;
	}

	@Override
	public int size() {
		return (int) this.idCounter;
	}

	/**
	 * Returns the physical position of the requested macro block in the file
	 *
	 * @param id
	 *            the logical id of the requested block
	 * @param macroBlockID
	 *            the id of the containing macro block
	 * @return the physical byte position of the macro block
	 */
	private long getMacroBlockPosition(final long id, final long macroBlockID) {
		// Returns the physical position of the given block ID
		// This depends on the macro block size and the number of
		// TLB entries in between (also depends on the macro block size=
		
		// FIXME: This computation does not work for out of order inserts, since it computes the number of TLB-entries to skip based on the logical id.
		return this.preambleSize + macroBlockID * this.macroBlockSize + this.tlb.getMapBlockCount(id) * this.blockSize;
	}

	/**
	 * Encodes a block position from physical block position and offset in block
	 *
	 * @param blockID
	 *            the id of the physical block
	 * @param entryNumber
	 *            the offset of the entry in the block
	 * @return blockID|offset
	 */
	private static long encodePhysicalAddress(final long blockID, final int entryNumber) {
		long physical = blockID << 8;
		final int b = Byte.toUnsignedInt((byte) entryNumber) & 0xFF;
		physical |= b;
		return physical;
	}

	/**
	 * Returns the macro block id for the given physical address.
	 *
	 * @param physicalAddress
	 *            the physical address
	 * @return the id of the macro block
	 */
	static long decodeBlockID(final long physicalAddress) {
		return physicalAddress >> 8;
	}

	/**
	 * Extracts the number of the entry within the block from the physical
	 * address.
	 *
	 * @param physicalAddress
	 *            the physical address
	 * @return the position within the macro block
	 */
	static int decodeEntryNumber(final long physicalAddress) {
		final byte en = (byte) (physicalAddress & 0xFF);
		if ( en < 0 )
			return unsignedToBytes(en);
		else
			return en;
	}

	private static int unsignedToBytes(final byte b) {
		return b & 0xFF;
	}

	@Override
	public synchronized void update(final Object id, final Object object, final boolean unfix)
		throws NoSuchElementException {
		final byte[] blockArray = ((Block) object).array;
		byte[] b;
		final long lid = (long) id;
		
		if ( lid < 0 || lid >= this.idCounter ) {
			throw new NoSuchElementException("Cannot update block. ID " + id + " not present in container.");
		}
		

		// b has to be padded to blockSize, if necessary!
		if ( blockArray.length < this.blockSize ) {
			b = new byte[this.blockSize];
			System.arraycopy(blockArray, 0, b, 0, blockArray.length);
		}
		else if ( blockArray.length > this.blockSize )
			throw new RuntimeException(
				"Block " + lid + " exceeds block size (" + this.blockSize + "): " + blockArray.length);
		else {
			b = blockArray;
		}

		byte[] compressed;
		try {
			compressed = this.compressor.compress(b);
		}
		catch (final Exception ex) {
			compressed = b;
		}
		long physicalAddress = getNextPhysicalAddress();  // calculates physical address: combination of block id and block offset.

		// Get macro block for insertion (to decide whether it is an out-of-order update)
		long requestedPhysicalID = 0;
		try {
			this.lock.lock();
			requestedPhysicalID = this.tlb.get(lid).physicalAddress;
		}
		catch (final IOException e) {
			throw new NoSuchElementException("There is no TLB entry for the given id " + lid);
		}
		finally {
			this.lock.unlock();
		}
		final long blockAddr = decodeBlockID(requestedPhysicalID);

		// check conditions for out-of-order updates
		if ( blockAddr != -1 && (blockAddr < this.physicalBlockIdCounter ||
			decodeEntryNumber(requestedPhysicalID) != this.currentMacroBlock.getCount()) ) { // Out-of-order update!
			try {
				logger.trace("Out-of-order insertion for id " + id + ":");
				this.blockBuffer.setOutOfOrder(true);
				outOfOrderUpdate(lid, compressed);
			}
			catch (final IOException e) {
				throw new NoSuchElementException("Error while updating block with id " + id + ": " + e.getMessage());
			}
			finally {
				this.blockBuffer.setOutOfOrder(false);
			}
		}
		else {
			if ( lid > this.tlb.getLastMappingId() + 1 ) { // Forward out-of-order update
				logger.trace(
					"Inserting new out-of-order block: " + lid + ". last id was " + this.tlb.getLastMappingId());
				// Fill entries into the tlb (to reserve place for the missing entries)
				final long lastMapping = this.tlb.getLastMappingId();
				final long diff = lid - lastMapping;
				// Add entry at the required position
				try {
					this.lock.lock();
					for (long i = 0; i < diff - 1; i++) {
						this.physicalPosition += this.tlb.addMapping(TLB.UNMAPPED_ID_PHYSICAL, this.physicalPosition, lastMapping + i + 1,
							this.physicalBlockIdCounter);
					}
				}
				catch (final IOException e) {
					throw new RuntimeException("Error inserting empty TLB entries for block " + lid);
				}
				finally {
					this.lock.unlock();
				}
			}
			try {
				this.lock.lock();
				physicalAddress = insert(compressed, physicalAddress); // Insert into the current macro block
				this.physicalPosition += this.tlb.addMapping(physicalAddress, this.physicalPosition, lid,
					this.physicalBlockIdCounter);
			}
			catch (final IOException e) {
				throw new NoSuchElementException("No element with id " + id);
			}
			finally {
				this.lock.unlock();
			}
		}
	}

	/**
	 * Inserts the given compressed block with the given logical at the given
	 * physical position.
	 *
	 * @param compressed
	 *            the compressed block
	 * @param physicalAddress
	 *            the estimated physical address
	 * @return the final physical address
	 */
	protected long insert(final byte[] compressed, long physicalAddress) throws IOException {
		if ( this.currentMacroBlock.freeSpace() < compressed.length ) {  // compressed does not fit completely into current macro block
			final long startBlock = this.physicalPosition / this.blockSize;
			if ( this.currentMacroBlock.freeSpace() > REFERENCE_ENTRY_SIZE ) { // compressed block fits partially into current macro block => Split into 'old' and 'next' => at least a reference entry has to fit
				final byte[] old = new byte[this.currentMacroBlock.freeSpace()];
				System.arraycopy(compressed, 0, old, 0, old.length);
				final byte[] next = new byte[compressed.length - old.length];
				System.arraycopy(compressed, old.length, next, 0, next.length);

				// add old to current
				this.currentMacroBlock.add(old, compressed.length);

				writeMacroBlock(this.currentMacroBlock, this.physicalBlockIdCounter, startBlock); // Write current macro block to disk
				this.physicalPosition += this.macroBlockSize;

				// create new macro block
				this.currentMacroBlock = createMacroBlock(this.macroBlockSize);
				this.physicalBlockIdCounter++;
				this.currentMacroBlock.addOffset(next); // Add remaining bytes as offset to new block
			}
			else { // if there is not enough place in the current macro block at all, put it completely to the nex node
				writeMacroBlock(this.currentMacroBlock, this.physicalBlockIdCounter, startBlock); // Write current macro block to disk
				this.physicalPosition += this.macroBlockSize;

				this.physicalBlockIdCounter++;
				physicalAddress = encodePhysicalAddress(this.physicalBlockIdCounter, 0);
				// create new macro block
				this.currentMacroBlock = createMacroBlock(this.macroBlockSize);
				this.currentMacroBlock.add(compressed, compressed.length);
			}
		}
		else { // block fits completely in current macro block
			this.currentMacroBlock.add(compressed, compressed.length);
		}
		return physicalAddress;
	}

	/**
	 * Processes an out-of-order update on a macro block.
	 *
	 * @param id
	 *            the number of the macro block
	 * @param b
	 *            the new block
	 * @throws java.io.IOException
	 */
	protected void outOfOrderUpdate(final long id, final byte[] b) throws IOException {
		final MacroBlockResult mbRes = getMacroBlockForID(id);
		final MacroBlock mb = mbRes.mb;
		final int entryNumber = mbRes.entryNumber;

		final EntryResult entry = mb.getEntry(entryNumber); // Get the entry
		if ( mb.isOverflowBlock(entryNumber) ) {
			MacroBlockResult nextMbRes;
			nextMbRes = getNextMacroBlock(mbRes.macroBlockID, mbRes.containerPosition);
			final MacroBlock nextMb = nextMbRes.mb; // Get the next macro block (next id is in next macro block)
			final float ratio = ((float) entry.array.length) / ((float) entry.length); // The ratio actual size to total size
			int newLeftSize = (int) (b.length * ratio); // Calculates the new size of the macro blocks partition (based on the original ratio)
			if ( newLeftSize - entry.array.length > mb.updateSpace() ) {
				newLeftSize = entry.array.length + mb.updateSpace(); // Set it to the maximum possible size
			}
			if ( newLeftSize < REFERENCE_ENTRY_SIZE ) {
				newLeftSize = REFERENCE_ENTRY_SIZE;
			}
			if ( b.length < REFERENCE_ENTRY_SIZE )
				throw new RuntimeException(
					"The size of block " + id + "[" + b.length + "] is smaller than a reference entry!");
			final int newRightSize = b.length - newLeftSize; // The rest is for the overflow node
			if ( newRightSize - nextMb.getOffset().length > nextMb.updateSpace() ) { // The new node does not fit into the next macro block => move it to the current macro block!
				// insert reference entry and add block to end of data base
				logger.trace("Block with id " + id + " exceeds overflowing macro block " + mbRes.macroBlockID +
					" => adding reference entry to current macro block " + this.physicalBlockIdCounter);
				addReferenceEntry(mb, entryNumber, b);
			}
			else { // The node fits

				logger.trace("Splitting block with id " + id + " to macro block " + mbRes.macroBlockID +
					" and macro block " + nextMbRes.macroBlockID);
				final byte[] leftPart = new byte[newLeftSize];
				System.arraycopy(b, 0, leftPart, 0, newLeftSize);
				final byte[] rightPart = new byte[newRightSize];
				System.arraycopy(b, newLeftSize, rightPart, 0, newRightSize);
				mb.replace(entryNumber, leftPart, b.length);
				nextMb.updateOverflow(rightPart);
			}
			// Write changed (next) macro block back
			final long pos = nextMbRes.containerPosition / this.blockSize;
			if ( nextMb != this.currentMacroBlock ) {
				writeMacroBlock(nextMb, nextMbRes.macroBlockID, pos, true);
			}
		}
		else {
			// Replace the block, if there is enough update space!!
			if ( b.length - entry.array.length <= mb.updateSpace() ) {
				logger.trace("Replacing block with id " + id + " in macro block " + mbRes.macroBlockID);
				mb.replace(entryNumber, b, b.length);
				logger.trace("New size of macro block " + mbRes.macroBlockID + ": " + mb.updateSpace());
			}
			else {
				// insert reference entry and add block to end of data base
				logger.trace("Block with id " + id + " exceeds macro block " + mbRes.macroBlockID +
					" => adding reference entry to macro block " + this.physicalBlockIdCounter);
				addReferenceEntry(mb, entryNumber, b);
			}
		}
		// Write changed macro block back
		final long pos = mbRes.containerPosition / this.blockSize;
		if ( mb != this.currentMacroBlock ) {
			writeMacroBlock(mb, mbRes.macroBlockID, pos, true);
		}
	}

	/**
	 * Adds a reference entry into the given macro block and adds the given
	 * block to the current macro block.
	 *
	 * @param mb
	 *            the macro block
	 * @param entryNumber
	 *            the entry number to be replaced
	 * @param b
	 *            the block
	 */
	private void addReferenceEntry(final MacroBlock mb, final int entryNumber, final byte[] b) throws IOException {
		// insert reference entry and add block to end of data base
		long lastWrittenId = this.tlb.getLastMappingId(); // The id of the last written block (required to calculate the number of written TLB entries)
		if ( this.tlb.getMapBlockCount(lastWrittenId) != this.tlb.getMapBlockCount(lastWrittenId + 1) ) {
			lastWrittenId = lastWrittenId + 1;
		}
		long physicalAddress = getNextPhysicalAddress(); // May be wrong because of full block!!
		// Adds the block to the current macro block (and creates a new macro block, if necessary)
		physicalAddress = insert(b, physicalAddress); // Corrected physical address after inserting
		mb.setReferenceEntry(entryNumber, new ReferenceEntry(physicalAddress, lastWrittenId));
		logger.trace("Physical address for reference entry: " + physicalAddress + ": " +
			decodeBlockID(physicalAddress) + " => " + decodeEntryNumber(physicalAddress));
	}

	/**
	 * Returns the next physical address to be used.
	 *
	 * @return the next physical address, consisting of current macro block id
	 *         and the entry number within the macro block
	 */
	protected long getNextPhysicalAddress() {
		return encodePhysicalAddress(this.physicalBlockIdCounter, this.currentMacroBlock.getCount());
	}

	/**
	 * Returns the macro block containing the given ID.
	 *
	 * @param lid
	 *            the ID of the block to be returned
	 * @return the macro block containing the block with the given ID
	 * @throws java.io.IOException
	 *             if an error occurs
	 */
	protected MacroBlockResult getMacroBlockForID(final long lid) throws IOException {
		long physicalAddress = -1L;
		try {
			this.lock.lock();
			physicalAddress= this.tlb.get(lid).physicalAddress;
		} finally {
			this.lock.unlock();
		}
		if ( physicalAddress == TLB.UNMAPPED_ID_PHYSICAL ) //current macro block
			throw new NoSuchElementException("No entry for ID : " + lid);
//			return new MacroBlockResult(this.physicalBlockIdCounter, this.currentMacroBlock.getCount(),
//				this.physicalPosition, this.currentMacroBlock);
		else
			return getMacroBlockForID(lid, physicalAddress, false);
	}

	/**
	 * Returns the macro block containing the given ID at the given physical
	 * address.
	 *
	 * @param lid
	 *            the ID of the block to be returned
	 * @param physicalAddress
	 *            the physical address of the block
	 * @return the macro block containing the block with the given ID
	 * @throws java.io.IOException
	 *             if an error occurs
	 */
	private MacroBlockResult getMacroBlockForID(final long lid, final long physicalAddress,
		final boolean isFollowingReference) throws IOException {
		final long macroBlockID = decodeBlockID(physicalAddress); //  check nextSwitchTLB > writtenTLB
		final int entryNumber = decodeEntryNumber(physicalAddress);

		long containerPosition = -1L;
		
		try {
			this.lock.lock();
			// Check macro block id of next TLB entry (if its the same, take this TLB to get the container position)
			containerPosition = getContainerPosition(lid, macroBlockID);
		} finally {
			this.lock.unlock();
		}
		

		MacroBlock b;
		// Read out block
		if ( containerPosition >= this.physicalPosition || macroBlockID == this.physicalBlockIdCounter ) {
			b = this.currentMacroBlock;
			logger.trace("Loading current Macro Block " + macroBlockID + " (ID, phys. address): " + lid + ", " +
				physicalAddress);
		}
		else {
			logger.trace("Loading Macro Block " + macroBlockID + " (ID, phys. address, container pos.): " + lid + ", " +
				physicalAddress + ", " + containerPosition);
			final long blockNumber = containerPosition / this.blockSize;
			try {
				final byte[] result = loadMacroBlock(blockNumber, isFollowingReference);
				b = createMacroBlock(result);
			}
			catch (final Exception ex) {
				logger.error("Error loading Macro Block " + macroBlockID + " (ID, phys. address, container pos.): " +
					lid + ", " + physicalAddress + ", " + containerPosition);
				throw new WrappingRuntimeException(ex);
			}
		}

		if ( b.isReferenceEntry(entryNumber) ) {
			final ReferenceEntry re = b.readReferenceEntry(entryNumber);
			this.blockBuffer.setOutOfOrder(true); // Disable buffer filling for out-of-order block reading
			logger.trace("Following reference entry: " + re.getPreviousWrittenId() + " => " +
				decodeBlockID(re.getPhysicalAddress()) + " : " + decodeEntryNumber(re.getPhysicalAddress()));
			final MacroBlockResult mbrs = getMacroBlockForID(re.getPreviousWrittenId(), re.getPhysicalAddress(), true);
			this.blockBuffer.setOutOfOrder(false);
			if ( mbrs.referenceEntry != null )
				return mbrs;
			else
				return mbrs.addReference(re);
		}
		else
			return new MacroBlockResult(macroBlockID, entryNumber, containerPosition, b);
	}

	/**
	 * Loads a macro block starting at the given block number
	 *
	 * @param startBlock
	 *            the logical block number of the first macro block block
	 * @return the macro block in bytes
	 */
	private byte[] loadMacroBlock(final long startBlock) {
		return loadMacroBlock(startBlock, false);
	}

	/**
	 * Loads a macro block starting at the given block number
	 *
	 * @param startBlock
	 *            the logical block number of the first macro block block
	 * @param skipTLBs
	 *            indicates if there could be leading TLB blocks before the
	 *            macro block. This could happen in case of out-of-order updates
	 *            and will be relevant for reference following.
	 * @return the macro block in bytes
	 */
	private byte[] loadMacroBlock(final long startBlock, final boolean skipTLBs) {
		
		if ( this.lastLoadedMacroBlockId == startBlock )
			return this.lastLoadedMacroBlock;
		
		if ( this.macroBlockBuffer.contains(this, startBlock) )
			return this.macroBlockBuffer.get(this, startBlock, null, true);
		
		logger.trace("Macro block cache miss -- start block: " + startBlock + "");
		final byte[] result = new byte[this.macroBlockSize];
		final byte[] block = new byte[this.blockSize];
		try {
			this.lock.lock();
			final int blockCount = this.macroBlockSize / this.blockSize;
			int skip = 0;
			for (int i = 0; i < blockCount; i++) {
				if ( (startBlock + i) >= this.raf.getNumSectors() )
					throw new RuntimeException("Error reading block " + (startBlock + i) + " from container (size: " +
						this.raf.getNumSectors() + ")!");
				this.blockBuffer.getBlock(block, startBlock + i + skip, false);
				if ( i == 0 && skipTLBs ) {
					while (isTLB(block)) {
						skip++;
						logger.trace("Skipped block -- new start block: " + startBlock + i + skip + "");
						this.blockBuffer.getBlock(block, startBlock + i + skip, false);
					}
				}
				System.arraycopy(block, 0, result, this.blockSize * i, block.length);
			}
		}
		catch (final Exception ex) {
			throw new WrappingRuntimeException(ex);
		}
		finally {
			this.lock.unlock();
		}
		this.lastLoadedMacroBlockId = startBlock;
		this.lastLoadedMacroBlock = result;
		updateMacroBlockBuffer(result, startBlock, null);
		return result;
	}

	/**
	 * Updates the macro block buffer.
	 *
	 * @param b
	 *            the binary macro block
	 * @param startBlock
	 *            the first block of the macro block in the container
	 * @param flush
	 *            the flush method for the container (required for updates)
	 */
	@SuppressWarnings("deprecation")
	private void updateMacroBlockBuffer(final byte[] b, final long startBlock,
		final xxl.core.functions.Function<Object, ?> flush) {
		final byte[] buf = new byte[b.length];
		System.arraycopy(b, 0, buf, 0, b.length);
		this.macroBlockBuffer.update(this, startBlock, buf, flush, true);
	}

	/**
	 * Writes a macro block starting at the given block number
	 *
	 * @param macroBlock
	 *            the macro block to be written
	 * @param startBlock
	 *            the start block for the macro block
	 */
	private void writeMacroBlock(final MacroBlock macroBlock, final long mbID, final long startBlock) {
		writeMacroBlock(macroBlock, mbID, startBlock, false);
	}

	/**
	 * Writes a macro block starting at the given block number
	 *
	 * @param macroBlock
	 *            the macro block to be written
	 * @param startBlock
	 *            the start block for the macro block
	 * @param updateBuffer
	 *            flag indicating if the buffer should be update
	 */
	@SuppressWarnings("deprecation")
	private void writeMacroBlock(final MacroBlock macroBlock, final long mbID, final long startBlock,
		final boolean updateBuffer) {
		macroBlock.getBytes(this.macroOutputBlock);
		if ( this.macroBlockBuffer.contains(this, startBlock) ) {
			updateMacroBlockBuffer(this.macroOutputBlock, startBlock,
				new xxl.core.functions.AbstractFunction<Object, Object>() {

					private static final long serialVersionUID = 1L;

					@Override
					public Object invoke(final Object argument0, final Object argument1) {
						writeToDisk((byte[]) argument1, mbID, (Long) argument0, true);
						return null;
					}
				});
		}
		else {
			updateMacroBlockBuffer(this.macroOutputBlock, startBlock, null);
			writeToDisk(this.macroOutputBlock, mbID, startBlock, updateBuffer);
		}
		if ( startBlock == this.lastLoadedMacroBlockId ) {
			this.lastLoadedMacroBlockId = -1L;
		}
	}

	private void writeToDisk(final byte[] macroBlock, final long mbID, final long startBlock,
		final boolean updateBuffer) {
		logger.trace("Writing macro block " + mbID + " to disk at block " + startBlock);
		try {
			this.lock.lock();
			for (int i = 0; i < this.macroBlockSize / this.blockSize; i++) {
				System.arraycopy(macroBlock, i * this.blockSize, this.outputBlock, 0, this.blockSize);
				this.raf.write(this.outputBlock, startBlock + i);
				if ( updateBuffer ) {
					this.blockBuffer.replace(startBlock + i, this.outputBlock);
				}
			}
		}
		finally {
			this.lock.unlock();
		}
	}

	// ================================================================================================================

	@Override
	public void suspend() {
		if ( !this.suspended ) {
			close();
			logger.debug("Container " + this.path + ".ctr suspended");
		}
		this.suspended = true;
	}

	@Override
	public void resume() {
		if ( this.suspended ) {
			this.raf.open(this.path + ".ctr");
			logger.debug("Container " + this.path + ".ctr resumed");
		}
		this.suspended = false;
	}

	@Override
	public boolean isSuspended() {
		return this.suspended;
	}

	@Override
	public Iterator<Long> idsBackwards() {
		return new Iterator<Long>() {
			long	pos	= FastCompressionContainer.this.idCounter-1;
			final long	min	= 0;
			Long next = computeNext();
			
			private Long computeNext() {
				Long result = null;
				for (; pos >= min && result == null; pos-- ) {
					if ( isUsed(pos) )
						result = pos;
				}
				return result;
			}
			

			@Override
			public boolean hasNext() {
				return next != null;
			}

			@Override
			public Long next() {
				Long result = next;
				next = computeNext();
				return result;
			}

			@Override
			public void remove() {
				throw new UnsupportedOperationException();
			}
		};
	}

	@Override
	public double getCompressionRatio() {
		return ((double) (this.physicalBlockIdCounter * this.macroBlockSize)) / (this.idCounter * this.blockSize);
	}

	@Override
	public long getAllocatedSpace() {
		return 0;
	}

	@Override
	public void allocateSpace(final long space) {
	}

	// ================================================================================================================

	/**
	 * Recovers the data from an illegally closed container (e.g. after a system
	 * crash). All data except the last unwritten macro block can be restored,
	 * setting the container in a legal state to continue writing and reading.
	 */
	public void recover() throws IOException {
		logger.info("Starting container recovery");
		final long tstart = System.currentTimeMillis();
		final TLBRecovery recovery = new TLBRecovery(this.raf, this.blockSize, this.preambleSize);
		logger.info("Starting TLB recovery");
		this.tlb = recovery.recover();
		this.tlb.setBlockBuffer(blockBuffer);
		logger.info("TLB recovery finished in " + (System.currentTimeMillis() - tstart) + " ms");

		final long fileSize = this.raf.getSectorSize() * this.raf.getNumSectors();
		long recoveryStart = this.preambleSize;
		long macroBlockId = 0L;
		int skip = 0;
		long entryID = 0;
		long lastContainerPosition = recoveryStart + ((fileSize - recoveryStart) / this.macroBlockSize);

		if ( recovery.getLastTLBPosition() != -1 ) {
			recoveryStart = recovery.getLastTLBPosition() + this.blockSize;
			lastContainerPosition = recoveryStart + ((fileSize - recoveryStart) / this.macroBlockSize);
			final long lastPhysicalAddress = recovery.getLastPhysicalAddress();
			final long lastMacroBlock = decodeBlockID(lastPhysicalAddress);
			final int lastEntryNumber = decodeEntryNumber(lastPhysicalAddress);

			for (int i = 0; i < this.tlb.writeBuffer.size(); i++) {
				entryID += this.tlb.writeBuffer.get(i).entries.size() *
					Math.pow(this.tlb.getBufferSize(), this.tlb.writeBuffer.get(i).level);
			}
			final long tlbEntryCount = entryID / this.tlb.getBufferSize();
			macroBlockId =
				(lastContainerPosition - this.preambleSize - (tlbEntryCount * this.blockSize)) / this.macroBlockSize;

			// Skip already written physical addresses from the current macro block
			// That is: There was a TLB-entry for some of this block's entries written, so skip them in TLB-reconstruction
			if ( lastMacroBlock == macroBlockId ) {
				skip = lastEntryNumber + 1;
			}
		}

		long position = recoveryStart;

		// Read all macro-blocks after the last written tlb-entry
		byte[] b = new byte[this.macroBlockSize];
		MacroBlock block = null;
		while (position + this.macroBlockSize <= fileSize) {
			b = loadMacroBlock(position / this.blockSize);
			block = createMacroBlock(b);

			// iterate positions of the entries
			for (int i = skip; i < block.getCount(); i++) {
				// TODO: This last parameter seems wrong!
				//                tlb.addMapping(encodePhysicalAddress(macroBlockId, i), lastContainerPosition, -1, physicalBlockIdCounter);
				this.tlb.addMapping(encodePhysicalAddress(macroBlockId, i), lastContainerPosition, -1, macroBlockId);
			}
			macroBlockId++;

			entryID += block.getCount() - skip;
			skip = 0;
			position += this.macroBlockSize;
		}
		// Load macro block
		if ( block != null ) {
			// If last block is overflow --> Kill it
			if ( block.isOverflowBlock(block.getCount() - 1) ) {
				this.tlb.removeLastMapping();
				block.setToLast(block.getCount() - 2);
				entryID--;
			}
			macroBlockId--;
			position -= this.macroBlockSize;
			this.currentMacroBlock = block;
		}
		else {
			this.currentMacroBlock = createMacroBlock(this.macroBlockSize);
		}
		// Set container state
		this.physicalBlockIdCounter = macroBlockId;
		this.idCounter = entryID;
		this.physicalPosition = position;
		logger.info("Container recovery finished in " + (System.currentTimeMillis() - tstart) + " ms");
	}

	// ================================================================================================================

	/**
	 * Prints debug output.
	 */
	protected void printState() {
		logger.debug(
			"idCounter:" + this.idCounter);/** Counter for macro blocks */
		logger.debug("MacroBlock ID:" + this.physicalBlockIdCounter);
		logger.debug("Physical position:" + this.physicalPosition);
		if ( this.tlb.writeBuffer.get(0).entries.size() > 0 ) {
			logger.debug("First TLB[0] entry:" + this.tlb.writeBuffer.get(0).entries.get(0));
			logger.debug("Last TLB[0] entry:" +
				this.tlb.writeBuffer.get(0).entries.get(this.tlb.writeBuffer.get(0).entries.size() - 1));
		}
	}

	@Override
	public int hashCode() {
		return this.path.hashCode();
	}

	// ================================================================================================================
	//
	// INNER CLASSES
	//
	// ================================================================================================================

	/**
	 * Represents a block address consisting of macro block ID and entry number.
	 */
	class MacroBlockResult {

		protected long				macroBlockID;
		protected int				entryNumber;
		protected long				containerPosition;
		protected MacroBlock		mb;
		protected ReferenceEntry	referenceEntry;						 // In case of an reference entry, this is the corresponding id

		MacroBlockResult(final long macroBlockID, final int entryNumber, final long containerPosition,
			final MacroBlock mb) {
			this.macroBlockID = macroBlockID;
			this.entryNumber = entryNumber;
			this.containerPosition = containerPosition;
			this.mb = mb;
		}

		MacroBlockResult addReference(final ReferenceEntry referenceEntry) {
			this.referenceEntry = referenceEntry;
			return this;
		}
	}

	// ================================================================================================================

}
