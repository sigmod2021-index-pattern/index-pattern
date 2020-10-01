package xxl.core.collections.containers.compression;

import java.io.IOException;
import java.util.NoSuchElementException;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import xxl.core.io.Block;
import xxl.core.util.Pair;

/**
 *
 */
public class ParallelCompressionContainer extends FastCompressionContainer {
	
	static final int NUM_COMPRESSORS = 4;
	
	private ExecutorService compressionPool = Executors.newFixedThreadPool(NUM_COMPRESSORS);
	
	private ExecutorService writePool = Executors.newSingleThreadExecutor();
	
	private BlockingQueue<Future<Pair<Long, byte[]>>> writeQueue = new ArrayBlockingQueue<>(8*NUM_COMPRESSORS);
	
	private boolean closed = false;

	private static final Logger logger = LoggerFactory
		.getLogger(ParallelCompressionContainer.class);

	/**
	 * Creates a new ParallelCompressionContainer instance
	 * 
	 * @param path
	 * @param blockSize
	 * @param spare
	 */
	public ParallelCompressionContainer(String path, int blockSize, float spare, int macroBlockBufferSize, boolean useDirectIO, boolean useBlockBuffer) {
		super(path, blockSize, spare, macroBlockBufferSize, useDirectIO, useBlockBuffer);
		start();
	}

	/**
	 * Creates a new ParallelCompressionContainer instance
	 * 
	 * @param path
	 * @param macroBlockSize
	 * @param blockSize
	 * @param compressor
	 * @param spare
	 */
	public ParallelCompressionContainer(String path, int macroBlockSize, int blockSize, Compressor compressor,
		float spare, int macroBlockBufferSize, boolean useDirectIO, boolean useBlockBuffer) {
		super(path, macroBlockSize, blockSize, compressor, spare, macroBlockBufferSize, useDirectIO, useBlockBuffer);
		start();
	}

	/**
	 * Creates a new ParallelCompressionContainer instance
	 * 
	 * @param path
	 * @param macroBlockSize
	 * @param blockSize
	 * @param spare
	 */
	public ParallelCompressionContainer(String path, int macroBlockSize, int blockSize, float spare, int macroBlockBufferSize, boolean useDirectIO, boolean useBlockBuffer) {
		super(path, macroBlockSize, blockSize, spare, macroBlockBufferSize, useDirectIO, useBlockBuffer);
		start();
	}

	/**
	 * Creates a new ParallelCompressionContainer instance
	 * 
	 * @param path
	 */
	public ParallelCompressionContainer(String path, int macroBlockBufferSize, boolean useDirectIO, boolean useBlockBuffer) {
		super(path, macroBlockBufferSize, useDirectIO, useBlockBuffer);
		start();
	}
	
	private void start() {
		this.closed = false;
		this.writePool.execute(new WriterRunnable());
	}
	
	/**
	 * @{inheritDoc}
	 */
	@Override
	public void close() {
		this.compressionPool.shutdown();
		while ( !compressionPool.isShutdown() ) {
			try {
				this.compressionPool.awaitTermination(1, TimeUnit.SECONDS);
			}
			catch (InterruptedException e) {
			}
		}
		this.closed = true;
		this.writePool.shutdown();
		while ( !writePool.isShutdown() ) {
			try {
				this.writePool.awaitTermination(1, TimeUnit.SECONDS);
			}
			catch (InterruptedException e) {
			}
		}
		super.close();
	}

	@Override
	public synchronized void update(final Object id, final Object object, final boolean unfix)
		throws NoSuchElementException {
		
		final long lid = (long) id;
		final Block block = (Block) object;
		 
		Future<Pair<Long, byte[]>> f = compressionPool.submit( new CompressionTask(lid, block));
		try {
//			logger.debug("Enquing compression task");
			writeQueue.put(f);
//			logger.debug("Finished enquing comression task.");
		}
		catch (InterruptedException e) {
			logger.error("Could not enqueue compression task");
		}
	}

	private void doWrite(long lid, byte[] compressed) {
//		logger.debug("Enter: doWrite");
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
				logger.trace("Out-of-order insertion for id " + lid + ":");
				this.blockBuffer.setOutOfOrder(true);
				outOfOrderUpdate(lid, compressed);
			}
			catch (final IOException e) {
				throw new NoSuchElementException("Error while updating block with id " + lid + ": " + e.getMessage());
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
						this.physicalPosition += this.tlb.addMapping(-1, this.physicalPosition, lastMapping + i + 1,
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
				throw new NoSuchElementException("No element with id " + lid);
			}
			finally {
				this.lock.unlock();
			}
		}
//		logger.debug("Exit: doWrite");
	}
	
	private class CompressionTask implements Callable<Pair<Long, byte[]>> {
		
		private final long lid;
		
		private final Block data;
		
		/**
		 * Creates a new CompressionTask instance
		 * @param data
		 */
		public CompressionTask(long lid, Block data) {
			this.lid = lid;
			this.data = data;
		}

		/**
		 * @{inheritDoc}
		 */
		@Override
		public Pair<Long, byte[]> call() throws Exception {
//			logger.debug("Compressing block");
			final byte[] blockArray = data.array;
			byte[] b;

			// b has to be padded to blockSize, if necessary!
			if ( blockArray.length < ParallelCompressionContainer.this.blockSize ) {
				b = new byte[ParallelCompressionContainer.this.blockSize];
				System.arraycopy(blockArray, 0, b, 0, blockArray.length);
			}
			else if ( blockArray.length > ParallelCompressionContainer.this.blockSize )
				throw new RuntimeException(
					"Block " + lid + " exceeds block size (" + ParallelCompressionContainer.this.blockSize + "): " + blockArray.length);
			else {
				b = blockArray;
			}

			byte[] compressed = ParallelCompressionContainer.this.compressor.compress(b);
			return new Pair<>(lid,compressed);
		}
	}
	
	private class WriterRunnable implements Runnable {

		/**
		 * @{inheritDoc}
		 */
		@Override
		public void run() {
			while (!closed || !writeQueue.isEmpty() ) {
//			while ( true ) {
				try {
					Future<Pair<Long, byte[]>> f = writeQueue.poll(1, TimeUnit.SECONDS);
					if ( f != null ) {
						Pair<Long, byte[]> result = f.get();
	//					logger.debug("Writing compressed data!");
						doWrite(result.getFirst(), result.getSecond());
					}
				} catch ( InterruptedException | ExecutionException ie ) {
					logger.error("Block could not be compressed.", ie);
					
				}
			}
		}
	}
	
	
}
