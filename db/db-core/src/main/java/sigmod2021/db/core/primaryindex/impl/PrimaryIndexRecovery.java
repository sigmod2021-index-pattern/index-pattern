package sigmod2021.db.core.primaryindex.impl;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sigmod2021.event.Event;
import xxl.core.collections.containers.io.SuspendableContainer;
import xxl.core.indexStructures.BPlusLink;
import xxl.core.indexStructures.BPlusLink.Node;
import xxl.core.indexStructures.BPlusTree.IndexEntry;

import java.util.*;
import java.util.Map.Entry;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 *
 */
public class PrimaryIndexRecovery {

    public static final int LEAF_LEVEL = 0;
    /** The logger */
    static final Logger log = LoggerFactory.getLogger(PrimaryIndexRecovery.class);

    public static RecoveryResult recoverTreePath(Function<Event, Long> keyFunction, final SuspendableContainer treeContainer) throws IllegalStateException {
        Iterator<RNode> bwIter = new BackwardIterator(treeContainer);

        // Nothing to do
        if (!bwIter.hasNext()) {
            return new RecoveryResult(RecoveryType.EMPTY, new TreeMap<>());
        }


        // Scan for fragments of regular shutdown (i.e., next neighbour points to negative id)
        log.debug("Checking for fragments of previous regular shutdown.");
        SortedMap<Integer, RNode> path = new TreeMap<>();
        while (bwIter.hasNext()) {
            RNode tmp = bwIter.next();
            log.debug("Inspecting node " + tmp.id + ": Level=" + tmp.node.level + ", nextNeightbour=" + tmp.node.nextNeighbor());
            if (tmp.node.nextNeighbor() != null && ((Long) tmp.node.nextNeighbor().id() <= 0)) {
                path.put(tmp.node.level, tmp);
            } else {
                break;
            }
        }
        log.debug("Found the following fragments: " + path);

        // We found a full path. we can simply use normal load operation.
        if (isFullFlank(path)) {
            log.debug("Found complete right flank in container. Regular load possible.");
            return new RecoveryResult(RecoveryType.REGULAR_LOAD, path);
        }
        // If we found only fragments: Remove them from container and trigger regular recovery
        else if (!path.isEmpty()) {
            log.debug("Found only fragments of regular shutdown. Removing and triggering regular recovery.");

            if (path.containsKey(LEAF_LEVEL)) {
                RNode leaf = path.get(LEAF_LEVEL);
                log.debug("Preserving incomplete leaf: " + leaf.id);
                leaf.node.nextNeighbor().initialize(leaf.id + 1);
//				leaf.node.nextNeighbor().initialize(-leaf.id);
                treeContainer.update(leaf.id, leaf.node);
                path.remove(LEAF_LEVEL);
            }

            List<Long> ids = path.values().stream().map(x -> x.id).collect(Collectors.toList());
            Collections.sort(ids, Comparator.comparingLong(x -> (Long) x).reversed());
            ids.forEach(x -> treeContainer.remove(x));
        }

        log.debug("Retrieving blocks of first 2 levels.");

        bwIter = new BackwardIterator(treeContainer);
        // Find at least level 0 and level 1
        path = begin(bwIter, treeContainer);

        // If we only encountered leaves in begin, we are done
        // Otherwise, find rightmost element level by level

        log.debug("Retrieved first levels: " + path);

        if (path.lastKey() > LEAF_LEVEL) {
            long oldSize = 0;
            do {
                oldSize = path.size();
                RNode oldTop = path.get(path.lastKey());
                Optional<RNode> res = findNextLevel(keyFunction, path.get(path.lastKey()), treeContainer);
                if (res.isPresent())
                    path.put(oldTop.node.level() + 1, res.get());

            } while (oldSize < path.size());
        }

        return new RecoveryResult(RecoveryType.RECOVER, path);
    }

    private static boolean isFullFlank(SortedMap<Integer, RNode> path) {
        if (path.isEmpty()) {
            return false;
        }
        boolean result = true;

        RNode root = path.get(path.lastKey());

        // Do we have a node for every level
        result &= (root.node.level() + 1 - path.size() == 0);
        result &= root.node.previousNeighbor() == null;

        for (Entry<Integer, RNode> e : path.entrySet()) {
            result &= e.getValue().node.nextNeighbor() != null;
            result &= ((Long) e.getValue().node.nextNeighbor().id()) <= 0;
        }
        return result;
    }

    private static Optional<RNode> findNextLevel(Function<Event, Long> keyFunction, RNode rightmostChild, SuspendableContainer treeContainer) {
        log.debug("Trying to find block for level " + (rightmostChild.node.level + 1));
        RNode work = rightmostChild;
        RNode result = null;

        // A node of the parent level is stored to the right of one of the current levels nodes, if no out-of-order occured.
        // We exploit this and cancel, if we find an out of order node
        while (result == null && work != null) {
            log.trace("Processing level " + work.node.level + "-node (" + work.id + ")");
            Long rightId = work.id + 1;
            // Check if there is a page to the right
            if (treeContainer.contains(rightId)) {
                RNode right = new RNode(rightId, (Node) treeContainer.get(rightId));

                long myMaxKey = getMaxKey(keyFunction, work);
                long rMaxKey = getMaxKey(keyFunction, right);

                // We found our candidate
                if (right.node.level() == work.node.level() + 1) {
                    log.debug("Found level-" + (work.node.level() + 1) + " node: " + rightId);

                    result = right;
                }
                // Out of order leaf
                else if (right.node.level() == 0 && rMaxKey < myMaxKey) {
                    throw new IllegalStateException("Cannot apply smart recovery. Detected out-of-order pages.");
                }
                // OK, move work to left sibling
                else if (right.node.level() == 0 && work.node.previousNeighbor() != null) {
                    log.trace("Moving to previous neighbour: " + work.node.previousNeighbor());
                    Long pId = (Long) work.node.previousNeighbor().id();
                    work = new RNode(pId, (Node) treeContainer.get(pId));
                }
                // OK, we reached the end -> Top level
                else if (right.node.level() == 0) {
                    work = null;
                }
                // Strange things
                else {
                    throw new IllegalStateException("Cannot apply smart recovery. Found strange page order. Level " + right.node.level() + " node right of level " + work.node.level() + " node.");
                }
            }
            // If not, move work to left sibling
            else if (work.node.previousNeighbor() != null) {
                Long pId = (Long) work.node.previousNeighbor().id();
                work = new RNode(pId, (Node) treeContainer.get(pId));
            }
            // OK, we reached the end -> Top level
            else {
                work = null;
            }
        }
        return Optional.ofNullable(result);
    }

    private static Long getMaxKey(Function<Event, Long> keyFunction, RNode node) {
        if (node.node.level() == LEAF_LEVEL) {
            return keyFunction.apply((Event) node.node.getLast());
        } else {
            return (Long) ((IndexEntry) node.node.getLast()).separator().sepValue();
        }
    }

    private static SortedMap<Integer, RNode> begin(Iterator<RNode> iter, final SuspendableContainer treeContainer) {
        SortedMap<Integer, RNode> result = new TreeMap<>();

        // Find at least a leaf and level 1
        while (iter.hasNext() && !(result.containsKey(LEAF_LEVEL) && result.containsKey(LEAF_LEVEL + 1))) {
            RNode chk = iter.next();
            IndexEntry nn = chk.node.nextNeighbor();

            if (nn == null || nn.id() == null || !treeContainer.contains(nn.id()) || nn.id().equals(chk.id))
                result.put(chk.node.level(), chk);
        }
        return result;
    }

    /**
     * Reconstructs the right flank from the nodes stored in the given container
     *
     * @param treeContainer
     *            the container to recover from
     * @return the recovered right tree flank
     */
    public static RecoveryResult recoverTreePathWithBruceForce(Function<Event, Long> keyFunction,
                                                               final SuspendableContainer treeContainer) {


        SortedMap<Integer, RNode> treePath = new TreeMap<>();

        // Assumption: ids are one-step ascending long values
        BPlusLink.Node node;
        BPlusLink.IndexEntry entry;
        @SuppressWarnings("unchecked") final Iterator<Long> idIterator = treeContainer.ids();
        while (idIterator.hasNext()) {
            Long id = idIterator.next();
            // Maybe there is no such entry
            if (!treeContainer.contains(id)) {
                break;
            }
            node = (BPlusLink.Node) treeContainer.get(id);

            if (!treePath.containsKey(node.level())) {
                // Search the right-most node for the current level
                while (node.nextNeighbor() != null) {
                    entry = node.nextNeighbor();
                    if (!entry.id().equals(id) && treeContainer.contains(entry.id())) {
                        id = (Long) entry.id();
                        node = entry.get();
                    } else {
                        node.setNextNeighbor(null);
                        //						treeContainer.update(id, node);
                        break;
                    }
                }
                // Put right-most entry to the past
                treePath.put(node.level(), new RNode(id, node));
            } else {
                // treePath contains the right-most __reachable__ node!
                // => check, if the current node is the right-most of all nodes accessible

                // Leaf node case
                if (node.level() == 0) {
                    // Key of the current node´s last entry
                    final Long currentKey = keyFunction.apply((Event) node.getLast());

                    // Key of the stored node´s last entry
                    final Long lastKey = keyFunction
                            .apply((Event) treePath.get(node.level()).node.getLast());

                    // If the current node is right of the last store, replace it in the treePath
                    if (currentKey.longValue() > lastKey.longValue()) {
                        treePath.put(node.level(), new RNode(id, node));
                    }
                }
                // Index node case
                else {
                    entry = (BPlusLink.IndexEntry) node.getLast(); // Last index entry of the node
                    final Long currentSepVal = (Long) entry.separator().sepValue();
                    final BPlusLink.IndexEntry lastEntry = (BPlusLink.IndexEntry) treePath.get(node.level())
                            .node.getLast();
                    final Long lastSepVal = (Long) lastEntry.separator().sepValue();

                    // If the current node is right of the last store, replace it in the treePath
                    if (currentSepVal.compareTo(lastSepVal) > 0) {
                        treePath.put(node.level(), new RNode(id, node));
                    }
                }
            }
        }
        return new RecoveryResult(RecoveryType.RECOVER, treePath);
    }


    public static enum RecoveryType {
        EMPTY, REGULAR_LOAD, RECOVER;
    }

    public static class RecoveryResult {
        private final RecoveryType type;
        private final SortedMap<Integer, RNode> treePath;

        /**
         * Creates a new RecoveryResult instance
         * @param type
         * @param treePath
         */
        public RecoveryResult(RecoveryType type, SortedMap<Integer, RNode> treePath) {
            this.type = type;
            this.treePath = treePath;
        }

        /**
         * @return the type
         */
        public RecoveryType getType() {
            return this.type;
        }

        /**
         * @return the treePath
         */
        public SortedMap<Integer, RNode> getTreePath() {
            return this.treePath;
        }
    }

    public static class RNode {
        final Long id;
        final Node node;

        /**
         * Creates a new RNode instance
         * @param id
         * @param node
         */
        public RNode(Long id, Node node) {
            this.id = id;
            this.node = node;
        }

        /**
         * @return the id
         */
        public Long getId() {
            return this.id;
        }

        /**
         * @return the node
         */
        public Node getNode() {
            return this.node;
        }

        /**
         * @{inheritDoc}
         */
        @Override
        public String toString() {
            return String.format("%d", this.id);
        }
    }

    private static class BackwardIterator implements Iterator<RNode> {

        private SuspendableContainer con;

        private Iterator<?> src;

        /**
         * Creates a new TABPlusRecovery.BackwardIDIterator instance
         */
        private BackwardIterator(SuspendableContainer con) {
            this.con = con;
            this.src = con.idsBackwards();
        }

        /**
         * @{inheritDoc}
         */
        @Override
        public boolean hasNext() {
            return src.hasNext();
        }

        /**
         * @{inheritDoc}
         */
        @Override
        public RNode next() {
            //System.err.println("Fetching next block");
            Long id = (Long) src.next();
            return new RNode(id, (Node) con.get(id));
        }

    }

}
