package xxl.core.collections.containers.io;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.OpenOption;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.nio.file.attribute.FileStoreAttributeView;
import java.util.Iterator;
import java.util.NoSuchElementException;

import com.sun.nio.file.ExtendedOpenOption;

import xxl.core.collections.containers.AbstractContainer;
import xxl.core.functions.Function;
import xxl.core.io.Block;
import xxl.core.io.converters.FixedSizeConverter;
import xxl.core.io.converters.LongConverter;
import xxl.core.util.WrappingRuntimeException;

/**
 * This class provides a container that is able to store blocks in a file.
 * There are some constructors that create a new container and
 * there are some other constructors that open an existing container.
 * All functionality concerning files can be exchanged by
 * passing different factory methods to the constructors (important
 * for the usage of own filesystems).
 * <p>
 * The container depends on five files: a container file, a meta file,
 * a reservedBitMap file, a updatedBitMap file and a freeList file.<br>
 * The container file is used for storing the blocks of this container. In
 * addition, the offset of an block in the container file determines the
 * id of the block in this container. Every block stored in the container
 * file takes <tt>blockSize</tt> bytes, therefore blocks that contains
 * more than <tt>blockSize</tt> bytes cannot be stored in this container.
 * The metaData file contains the blockSize and the number of reserved blocks.
 * The reservedBitMap file is a fat file of this container.
 * It stores for each block whether it is already in use, i.e. cannot be reserved.
 * In order to store this table, eight bits are joined to an <tt>int</tt> value.
 * For this reason the <tt>n</tt>th bit of this file is stored as the <tt>(n%8)</tt>
 * th bit of the <tt>(n/8)</tt> th <tt>byte</tt> value of this file. The name
 * of this file is determined by the <tt>String prefix</tt> and the file
 * extension <tt>.rbm</tt>. Similar to the reservedBitMap file, the
 * updatedBitMap file stores for each block whether an update has been
 * performed so that it is possible to get the block.
 * The freeList file stores the offsets of removed blocks. Whenever a block
 * should be written to this container, the freeList file is checked, whether
 * there are empty blocks in the container file. If the freeList file contains
 * offsets of removed blocks the new block will be stored in an empty space of
 * the container file, else the container file will be enlarged.
 * <p>
 * Earlier versions of xxl worked with only three files. Such Containers
 * are automatically migrated to the five files version (at the first
 * use).
 * <p>
 * Example usage (1).
 * <pre>
 *     // create a new block file container with ...
 *
 *     BlockFileContainer container = new BlockFileContainer(
 *
 *         // files having the file name "BlockFileContainer"
 *
 *         "BlockFileContainer",
 *
 *         // a block size of 4 bytes (size of a serialized integer)
 *
 *         4
 *     );
 *
 *     // insert 10 blocks containing the integers between 0 and 9
 *
 *     for (int i = 0; i < 10; i++) {
 *
 *         // create a new block
 *
 *         Block block = new Block(4);
 *
 *         // catch IOExceptions
 *
 *         try {
 *
 *             // write the value of i to the block
 *
 *             block.dataOutputStream().write(i);
 *         }
 *         catch (IOException ioe) {
 *             System.out.println("An I/O error occured.");
 *         }
 *
 *         // insert the block into the block file container
 *
 *         container.insert(block);
 *     }
 *
 *     // get the ids of all elements in the container
 *
 *     Iterator iterator = container.ids();
 *
 *     // print all elements of the container
 *
 *     while (iterator.hasNext()) {
 *
 *         // get the block from the container
 *
 *         Block block = (Block)container.get(iterator.next());
 *
 *         // catch IOExceptions
 *
 *         try {
 *
 *             // print the data of the block
 *
 *             System.out.println(block.dataInputStream().read());
 *         }
 *         catch (IOException ioe) {
 *             System.out.println("An I/O error occured.");
 *         }
 *     }
 *
 *     // close the open container and clear its file after use
 *
 *     container.close();
 *     container.clear();
 *     container.delete();
 * </pre>
 *
 * @see xxl.core.collections.containers.Container
 * @see IOException
 * @see Iterator
 * @see NoSuchElementException
 * @see RandomAccessFile
 * @see WrappingRuntimeException
 */
public class FCBlockFileContainer extends AbstractContainer {

	/**
	 * The name for the files of this container. The file names of the
	 * five files a block file container consists of differ only in their
	 * file extensions.
	 */
	private final Path   dir;
	
	private final String name;
	
	private final boolean directIO;
	
	/**
	 * Extensions of the files used.
	 */
	public static final String EXTENSIONS[] = new String[] {".mtd", ".rbm", ".ubm", ".flt", ".ctr"};
	
	/**
	 * Constant for the mdf-file (inside the EXTENSIONS-array).
	 */
	public static final int MTD_FILE = 0;

	/**
	 * Constant for the rbm-file (inside the EXTENSIONS-array).
	 */
	public static final int RBM_FILE = 1;

	/**
	 * Constant for the ubm-file (inside the EXTENSIONS-array).
	 */

	public static final int UBM_FILE = 2;
	/**
	 * Constant for the flt-file (inside the EXTENSIONS-array).
	 */

	public static final int FLT_FILE = 3;
	/**
	 * Constant for the ctr-file (inside the EXTENSIONS-array).
	 */

	public static final int CTR_FILE = 4;
	
	/**
	 * Returns the number of files which are used by the container.
	 * @return number of files
	 */
	public static int getNumberOfFiles() {
		return EXTENSIONS.length;
	}
	
	/**
	 * Returns a string array with the filenames which are used by the container.
	 * @param prefix the beginning of each filename.
	 * @return String array containing the filenames.
	 */
	public static String[] getFilenamesUsed(String name) {
		String ar[] = new String[EXTENSIONS.length];
		for (int i=0; i<EXTENSIONS.length; i++)
			ar[i] = new String(name+EXTENSIONS[i]);
		return ar;
	}

	/**
	 * The container file of this container. The container file is used
	 * for storing the blocks of this container. In addition, the offset
	 * of an block in the container file determines the id of the block in
	 * this container. The name of the container file is determined by the
	 * <tt>String prefix</tt> and the file extension <tt>.ctr</tt>.
	 */
	protected FileChannel container;

	/**
	 * The metaData file contains the blockSize and the number of reserved
	 * blocks. The name of the container file is determined by the
	 * <tt>String prefix</tt> and the file extension <tt>.mtd</tt>.
	 */
	protected RandomAccessFile metaData;

	/**
	 * A fat file of this container. The reservedBitMap file stores a bit
	 * table that shows for each block whether it can be reserved (false)
	 * or not (true). In order to store this table, eight bits are joined
	 * to an <tt>int</tt> value. For this reason the <tt>n</tt>th bit of
	 * this file is stored as the <tt>(n%8)</tt>th bit of the <tt>(n/8)</tt>
	 * th <tt>byte</tt> value of this file. The name of this file is
	 * determined by the <tt>String prefix</tt> and the file extension
	 * <tt>.rbm</tt>.
	 */
	protected RandomAccessFile reservedBitMap;

	/**
	 * A fat file of this container. The updatedBitMap file stores a bit
	 * table that shows for each block whether it can be get (true) or
	 * not (false). In order to store this table, eight bits are joined
	 * to an <tt>int</tt> value. For this reason the <tt>n</tt>th bit of
	 * this file is stored as the <tt>(n%8)</tt>th bit of the <tt>(n/8)</tt>
	 * th <tt>byte</tt> value of this file. The name of this file is
	 * determined by the <tt>String prefix</tt> and the file extension
	 * <tt>.ubm</tt>.
	 */
	protected RandomAccessFile updatedBitMap;

	/**
	 * The freeList file of this container. The freeList file stores the
	 * offsets of removed blocks. Whenever a block should be written to
	 * this container, the freeList file is checked, whether there are
	 * empty blocks in the container file. If the freeList file contains
	 * offsets of removed blocks the new block will be stored in an empty
	 * space of the container file, else the container file will be
	 * enlarged. The name of the freeList file is determined by the
	 * <tt>String prefix</tt> and the file extension <tt>.flt</tt>.
	 */
	protected RandomAccessFile freeList;

	/**
	 * The size reserved for storing a block in this container. Every
	 * block stored in the container file takes <tt>blockSize</tt> bytes,
	 * therefore blocks that contains more than <tt>blockSize</tt> bytes
	 * cannot be stored in this container.
	 */
	protected int blockSize;

	/**
	 * The number of blocks stored in this container.
	 */
	protected int size;
	
	private long virtualFileLength;
	
	
	/**
	 * Constructs an empty BlockFileContainer that is able to store blocks
	 * with a maximum size of <tt>blockSize</tt> bytes. The given
	 * <tt>String prefix</tt> specifies the names of the files the are
	 * used for storing the elements of the container. When using existing
	 * files to store the container their data will be overwritten.
	 * <p>
	 * This constructor is useful if you want to keep your file in
	 * a special self developed filesystem.
	 *
	 * @param prefix specifies the names of the files the container
	 *        consists of.
	 * @param blockSize the size reserved for storing a block in the
	 *        container file.
	 * @param fso Provides an object which performs the operations on the filesystem.
	 * @throws IOException 
	 */
	public FCBlockFileContainer ( Path dir, String name, int blockSize, boolean directIO) throws IOException {
		this.dir = dir;
		this.name = name;
		this.blockSize = blockSize;
		this.directIO = directIO;
		this.virtualFileLength = 0;
		openFiles();
		reset();
	}
	
	/**
	 * Constructs an empty BlockFileContainer that is able to store blocks
	 * with a maximum size of <tt>blockSize</tt> bytes. The given
	 * <tt>String prefix</tt> specifies the names of the files the are
	 * used for storing the elements of the container. When using existing
	 * files to store the container their data will be overwritten.
	 * <p>
	 * This constructor is useful if you want to keep your file in
	 * a special self developed filesystem.
	 *
	 * @param prefix specifies the names of the files the container
	 *        consists of.
	 * @param blockSize the size reserved for storing a block in the
	 *        container file.
	 * @param fso Provides an object which performs the operations on the filesystem.
	 * @throws IOException 
	 */
	public FCBlockFileContainer ( Path dir, String name, boolean directIO) throws IOException {
		this.dir = dir;
		this.name = name;
		this.directIO = directIO;
		this.virtualFileLength = 0;
		open();
	}

	/**
	 * Opens the five container files using the given Factory.
	 * @throws IOException 
	 */
	protected void openFiles() throws IOException {
		OpenOption[] oos = directIO ? new OpenOption[] { StandardOpenOption.CREATE, StandardOpenOption.READ, StandardOpenOption.WRITE, ExtendedOpenOption.DIRECT } : 
									  new OpenOption[] { StandardOpenOption.CREATE, StandardOpenOption.READ, StandardOpenOption.WRITE };
		
		this.container = FileChannel.open(dir.resolve(name + EXTENSIONS[CTR_FILE]), oos);
		this.metaData = new RandomAccessFile(dir.resolve(name + EXTENSIONS[MTD_FILE]).toString(), "rw");
		this.reservedBitMap = new RandomAccessFile(dir.resolve(name + EXTENSIONS[RBM_FILE]).toString(), "rw");
		this.updatedBitMap = new RandomAccessFile(dir.resolve(name + EXTENSIONS[UBM_FILE]).toString(), "rw");
		this.freeList = new RandomAccessFile(dir.resolve(name + EXTENSIONS[FLT_FILE]).toString(), "rw");
		this.virtualFileLength = container.size();
	}


	/**
	 * Opens the files of this container and restores the state of this
	 * container. This method expects the serialized state of the
	 * container at the end of the fat file. After restoring the state of
	 * the container, the serialized data is removed from the fat file.
	 */
	protected void open () {
		if (this.container==null)
			try {
				openFiles();
				this.blockSize = metaData.readInt();
				this.size = metaData.readInt();
			}
			catch (IOException ie) {
				throw new WrappingRuntimeException(ie);
			}
	}

	/**
	 * Returns a converter for the ids generated by this container. A
	 * converter transforms an object to its byte representation and vice
	 * versa - also known as serialization in Java.<br>
	 * Because of using the offset in the container file (<tt>long</tt>
	 * value) as id, this method always returns a <tt>LongConverter</tt>.
	 *
	 * @return a converter for serializing the identifiers of the
	 *         container.
	 */
	@Override
	public FixedSizeConverter<Long> objectIdConverter () {
		return LongConverter.DEFAULT_INSTANCE;
	}

	/**
	 * Returns the size of the ids generated by this container in bytes,
	 * which is 8.
	 * @return 8
	 */
	@Override
	public int getIdSize() {
		return LongConverter.SIZE;
	}

	/**
	 * Returns the size reserved for storing a block in this container.
	 * Every block stored in the container file takes <tt>blockSize</tt>
	 * bytes, therefore blocks that contains more than <tt>blockSize</tt>
	 * bytes cannot be stored in this container.
	 *
	 * @return the size reserved for storing a block in this container.
	 */
	public int blockSize () {
		return blockSize;
	}

	/**
	 * Resets this container and any files associated with it.<br>
	 * This implementation sets the length of the associated files to
	 * <tt>0</tt>. Thereafter the size and maximum offset of this
	 * container are corrected.
	 */
	public void reset () {
		open();
		try {
			container.truncate(0);
			reservedBitMap.setLength(0);
			updatedBitMap.setLength(0);
			freeList.setLength(0);
			size = 0;
			virtualFileLength = 0;
		}
		catch (IOException ie) {
			throw new WrappingRuntimeException(ie);
		}
	}

	/**
	 * Removes all elements from the Container. After a call of this
	 * method, <tt>size()</tt> will return 0.<br>
	 * This implementation only calls the <tt>reset()</tt> method.
	 */
	@Override
	public void clear () {
		reset();
	}

	/**
	 * Closes the Container and releases its associated files. But before
	 * closing the meta file, the serialized state of this container must
	 * be appended. Therefore, the values of the fields <tt>size</tt> and
	 * <tt>blockSize</tt> are append to the meta file. A closed container
	 *  can be implicitly reopened by a consecutive call to one of its
	 *  methods.
	 */
	@Override
	public void close () {
		if (this.container!=null)
			try {
				container.close();
				container = null;
				
				metaData.seek(0);
				metaData.writeInt(blockSize);
				metaData.writeInt(size);
				metaData.close();
				
				reservedBitMap.close();
				updatedBitMap.close();
				freeList.close();
			}
			catch (IOException ie) {
				throw new WrappingRuntimeException(ie);
			}
	}

	/**
	 * Returns <tt>true</tt> if the container contains a block for the identifier
	 * <tt>id</tt>.<br>
	 * This implementation checks whether the updatedBitMap files contains
	 * an entry for the offset specified by <tt>id</tt>.
	 *
	 * @param id identifier of the block.
	 * @return true if the container has updated a block for the specified
	 *         identifier.
	 */
	@Override
	public boolean contains (Object id) {
		open();
		try {
			long offset = ((Number)id).longValue();

			if (offset+blockSize>virtualFileLength)
				return false;

			updatedBitMap.seek(offset/blockSize/8);
			return (updatedBitMap.read()&(1<<(offset/blockSize%8)))!=0;
		}
		catch (IOException ie) {
			throw new WrappingRuntimeException(ie);
		}
	}

	/**
	 * Returns the block associated to the identifier <tt>id</tt>. An
	 * exception is thrown when the desired block is not found via contains.
	 * In this implementation the parameter unfix has no function because
	 * the container is unbuffered.
	 *
	 * @param id identifier of the block.
	 * @param unfix signals whether the object can be removed from the
	 *        underlying buffer.
	 * @return the block associated to the specified identifier.
	 * @throws NoSuchElementException if the desired block is not found.
	 */
	@Override
	public Object get (Object id, boolean unfix) throws NoSuchElementException {
		open();
		try {
			byte [] array = new byte [blockSize];

			if (!contains(id))
				throw new NoSuchElementException();

			container.position(((Number)id).longValue());
			readFully(container, ByteBuffer.wrap(array));
			return new Block(array, 0, blockSize);
		}
		catch (IOException ie) {
			throw new WrappingRuntimeException(ie);
		}
	}

	/**
	 * Returns an iterator that delivers all the identifiers of
	 * the container that are in use.
	 *
	 * @return an iterator of all identifiers used by this container.
	 */
	public Iterator<Long> ids () {
		open();
		return new Iterator<Long> () {
			Long id = Long.valueOf(-blockSize), nextId;
			boolean removeable = false;

			@Override
			public boolean hasNext () {
				for (removeable = false; !isUsed(nextId = Long.valueOf(id.longValue()+blockSize)); id = nextId)
					if (nextId.longValue()+blockSize>virtualFileLength)
						return false;
				return true;
			}

			@Override
			public Long next () throws NoSuchElementException {
				if (!hasNext())
					throw new NoSuchElementException();
				removeable = true;
				return id = nextId;
			}

			@Override
			public void remove () throws IllegalStateException {
				if (!removeable)
					throw new IllegalStateException();
				FCBlockFileContainer.this.remove(id);
				removeable = false;
			}
		};
	}


	/**
	 * Checks whether the <tt>id</tt> has been returned previously by a
	 * call to insert or reserve and hasn't been removed so far.
	 * This implementation checks whether the reservedBitMap files contains
	 * an entry for the offset specified by <tt>id</tt>.
	 *
	 * @param id the id to be checked.
	 * @return <tt>true</tt> exactly if the <tt>id</tt> is still in use.
	 */
	@Override
	public boolean isUsed (Object id) {
		open();
		try {
			long offset = ((Number)id).longValue();

			if (offset+blockSize>virtualFileLength)
				return false;
			reservedBitMap.seek(offset/blockSize/8);
			return (reservedBitMap.read()&(1<<(offset/blockSize%8)))!=0;
		}
		catch (IOException ie) {
			throw new WrappingRuntimeException(ie);
		}
	}

	/**
	 * Removes the block with identifier <tt>id</tt>. An exception is
	 * thrown when a block with an identifier <tt>id</tt> is not in the
	 * container. After a call of <tt>remove()</tt> all the iterators (and
	 * cursors) can be in an invalid state.<br>
	 * This implementation clears the entry for the block in both fat files
	 * and adds <tt>id</tt> to the freeList file.
	 *
	 * @param id an identifier of a block.
	 * @throws NoSuchElementException if a block with an identifier
	 *         <tt>id</tt> is not in the container.
	 */
	@Override
	public void remove (Object id) throws NoSuchElementException {
		open();
		try {
			long offset = ((Number)id).longValue();
			int b;

			if (!isUsed(id))
				throw new NoSuchElementException();
			if (--size==0)
				reset();
			else {
				reservedBitMap.seek(offset/blockSize/8);
				b = reservedBitMap.read();
				reservedBitMap.seek(reservedBitMap.getFilePointer()-1);
				reservedBitMap.write(b&~(1<<(offset/blockSize%8)));
				updatedBitMap.seek(offset/blockSize/8);
				b = updatedBitMap.read();
				updatedBitMap.seek(updatedBitMap.getFilePointer()-1);
				updatedBitMap.write(b&~(1<<(offset/blockSize%8)));
				// Truncate end of file
				if (offset+blockSize==virtualFileLength) {
					while (!isUsed(Long.valueOf(offset -= blockSize)));
					reservedBitMap.setLength(offset/blockSize/8+1);
					updatedBitMap.setLength(offset/blockSize/8+1);
					
					virtualFileLength = offset+blockSize;
					container.truncate(virtualFileLength);
				}
				else {
					freeList.seek(freeList.length());
					freeList.writeLong(offset);
				}
			}
		}
		catch (IOException ie) {
			throw new WrappingRuntimeException(ie);
		}
	}

	/**
	 * Reserves an id for subsequent use.
	 * This implementation sets in the reservedBitMap file the
	 * appropriate bit for the id returned by this method.
	 *
	 * @param getObject A parameterless function providing the object for
	 * 			that an id should be reserved. Not used by this
	 *			implementation.
	 * @return the reserved id.
	*/
	@SuppressWarnings({ "rawtypes", "deprecation" })
	@Override
	public Long reserve (Function getObject) {
		open();
		try {
			long offset;
			int b;

			for (;;) {
				if (freeList.length()==0) {
					offset = virtualFileLength;
					virtualFileLength += blockSize;
					break;
				}
				freeList.seek(freeList.length()-8);
				offset = freeList.readLong();
				freeList.setLength(freeList.length()-8);
				if (offset+blockSize<=virtualFileLength)
					break;
			}
			reservedBitMap.seek(offset/blockSize/8);
			if (reservedBitMap.getFilePointer()==reservedBitMap.length()) {
				reservedBitMap.write(1);
				updatedBitMap.seek(updatedBitMap.length());
				updatedBitMap.write(0);
			}
			else {
				b = reservedBitMap.read();
				reservedBitMap.seek(reservedBitMap.getFilePointer()-1);
				reservedBitMap.write(b|(1<<(offset/blockSize%8)));
			}
			size++;
			
			return offset;
		}
		catch (IOException ie) {
			throw new WrappingRuntimeException(ie);
		}
	}

	/**
	 * Returns the number of elements of the container. In other words,
	 * the number of set bits in the updatedBitMap file.
	 *
	 * @return the number of elements.
	 */
	@Override
	public int size () {
		return size;
	}

	/**
	 * Overwrites an existing (id,*)-element by (id, object). This method
	 * throws an exception if a block with an identifier <tt>id</tt> does
	 * not exist in the container (checked via isUsed). The parameter <tt>unfix</tt>
	 * has no function because this container is unbuffered.
	 *
	 * @param id identifier of the element.
	 * @param object the new block that should be associated to
	 *        <tt>id</tt>.
	 * @param unfix signals a buffered container whether the block can be
	 *        removed from the underlying buffer.
	 * @throws NoSuchElementException if a block with an identifier
	 *         <tt>id</tt> does not exist in the container.
	 */
	@Override
	public void update (Object id, Object object, boolean unfix) throws NoSuchElementException {
		open();
		try {
			long offset = ((Number)id).longValue();
			Block block = (Block)object;
			int b;

			if (offset+blockSize>virtualFileLength)
				throw new NoSuchElementException();
			updatedBitMap.seek(offset/blockSize/8);
			b = updatedBitMap.read();
			if ((b&(1<<(offset/blockSize%8)))==0) {
				if (!isUsed(id))
					throw new NoSuchElementException();
				updatedBitMap.seek(updatedBitMap.getFilePointer()-1);
				updatedBitMap.write(b|(1<<(offset/blockSize%8)));
			}
			if (block.size>blockSize)
				throw new IllegalArgumentException("Block too large: defined block size is " + blockSize + ", actual block size is "+block.size + ".");
			if (blockSize>block.array.length-block.offset) {
				byte [] array = new byte[blockSize];

				System.arraycopy(block.array, block.offset, array, 0, block.size);
				block = new Block(array);
			}
			container.position(offset);
			writeFully(container, ByteBuffer.wrap(block.array,block.offset,blockSize));
		}
		catch (IOException ie) {
			throw new WrappingRuntimeException(ie);
		}
	}

	/**
	 * Deletes the container. If necessary, the container is closed before.
	 */
	public void delete() {
		close();
		// delete the files of the container
		for (int i=0; i<FCBlockFileContainer.getNumberOfFiles(); i++)
			try {
				Files.deleteIfExists(dir.resolve(name + EXTENSIONS[i]));
			}
			catch (IOException e) {
			}
	}
	
	private void readFully(FileChannel channel, ByteBuffer buffer) throws IOException {
		int read = 0; 
		while ( read >= 0 && buffer.hasRemaining() )
			read = channel.read(buffer);
		
		assert !buffer.hasRemaining();
	}
	
	private void writeFully(FileChannel channel, ByteBuffer buffer) throws IOException {
		while ( buffer.hasRemaining() )
			channel.write(buffer);
		
		assert !buffer.hasRemaining();
	}
}
