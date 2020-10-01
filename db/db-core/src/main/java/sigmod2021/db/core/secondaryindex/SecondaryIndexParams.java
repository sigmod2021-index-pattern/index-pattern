package sigmod2021.db.core.secondaryindex;

public class SecondaryIndexParams {

    // ================= Secondary index constants ====================

    /**
     * The size of main memory (in bytes) to be used for external sorting
     * during queries on the secondary index.
     */
    public static int SORT_MEMORY = 10 * 1024 * 1024;

    /**
     * Number of levels in cache.
     * E.g. 16 levels with 8 byte key and 12 byte TID
     * result in a cache size of 1.25 MB in main memory
     */
    public static int COLA_CHACHE_SIZE = 8;

    /**
     * Number of bits in the bloom filter
     */
    public static int BLOOM_FILTER_SIZE = 256;

    /**
     * Index for living events enabled
     */
    public static boolean LIVE_INDEXING = false;

    /**
     * Type checking enabled
     */
    public static boolean TYPE_CHECKING = false;
}
