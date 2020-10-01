package sigmod2021.esp.bridges.nat.epa.impl.aggregator;

import sigmod2021.esp.api.bridge.EPACallback;
import sigmod2021.esp.bridges.nat.epa.impl.aggregator.NativeAggregator.AggregateFunctionHolder;
import sigmod2021.event.Event;
import sigmod2021.event.impl.SimpleEvent;

import java.io.Serializable;

/**
 * Implements the Agg-2-3-Tree for calculating patial aggregates.
 */
public class A23Tree implements Serializable {

    private static final long serialVersionUID = 1L;

    /**
     * The pattern under which this tree is stored
     */
    private final Object partitionId;

    /**
     * The aggregates to be calculated
     */
    private final AggregateFunctionHolder[] aggregates;

    /**
     * The skeleton used for empty nodes -- already holding group values
     */
    private final Object[] skeleton;

    /**
     * The callback for results
     */
    private final EPACallback callback;

    /**
     * The root of the tree
     */
    private A23TNode root;

    /**
     * The last update timestamp
     */
    private long lastUpdate = Long.MIN_VALUE;

    /**
     * Constructs a new A23Tree-instance
     *
     * @param pattern    The pattern under which this tree is stored
     * @param aggregates The aggregates to be calculated
     * @param skeleton   The skeleton used for empty nodes -- already holding group
     *                   values
     * @param osName     The name under which results are submitted
     */
    public A23Tree(Object partitionId, AggregateFunctionHolder[] aggregates, Object[] skeleton, EPACallback callback) {
        super();
        this.partitionId = partitionId;
        this.aggregates = aggregates;
        this.skeleton = skeleton;
        this.callback = callback;
    }

    /**
     * Creates and insersts a new node from the given event keeping a freshly
     * initialized partial aggregate.
     *
     * @param event new event to insert
     */

    public void insert(final Event event) {
        A23TNode node = new A23TNode(event);

        // Tree is empty
        if (root == null) {
            root = node;
            lastUpdate = event.getT1();
        }
        // simple case: new node fits perfectly
        else if (node.uLimit == root.uLimit) {
            combineNodes(node, root);
        }
        // We have more than the root node
        else if (root.nodeCount != 0) {
            if (node.uLimit > root.uLimit)
                root.appendNode(node);
            else
                root.insertNode(node);
        }
        // We only have the root node
        else {
            if (root.uLimit < node.uLimit) {
                node.addNode(this.root);
                node.addNode(new A23TNode(node.uLimit));
                root = node;
            } else {
                root.addNode(node);
                root.addNode(new A23TNode(root.uLimit));
            }
        }
    }

    /**
     * @return the lastUpdate timestamp for this tree
     */
    public long getLastUpdate() {
        return lastUpdate;
    }

    /**
     * Collects all recently expired events for an timestamp t, sends them to
     * the output-receiver and removes expired nodes from this tree
     *
     * @param t the determining timestamp
     */
    public void forwardResults(final long t) {
        if (root == null)
            return;

        // Walk down to the child level
        A23TNode node = root;
        int i = 0;
        while (node.nodeCount > 0 && node.uLimit >= t) {
            i = 0;
            do {
                node = node.nodes[i++];
            } while (node.uLimit < t);
        }
        if (node.nodeCount == 0 && node.uLimit == t) {
            node.lookUp(t);
            // Clean-Up
            deleteExpiredNodes(t);
        }
    }

    /**
     * Finds the pending partial aggregates for an timestamp t. This partial
     * aggregate is the split at t. The left part of it becomes a finished
     * partial aggregate while the right part of it is kept. The result is sent
     * to the output-receiver and expired nodes are removed from this tree.
     *
     * @param t the determining timestamp
     */
    public void forwardPendingResults(final long t) {
        if (root == null)
            return;

        // Walk down to the child level
        A23TNode node = root;
        int i = 0;
        while (node.nodeCount > 0 && node.uLimit >= t) {
            i = 0;
            do {
                node = node.nodes[i++];
            } while (node.uLimit < t);
        }
        if (node.nodeCount == 0 && node.uLimit > t) {
            node.lookUp(t);
            // Clean-Up
            deleteExpiredNodes(t);
        }
    }

    /**
     * @return true if this tree is empty, false otherwise
     */
    public boolean isEmpty() {
        return root == null;
    }

    /**
     * @return the group pattern used to store this tree
     */
    public Object getPartitionId() {
        return partitionId;
    }

    /**
     * Removes the expired nodes for the given timestamp
     *
     * @param t the determining timestamp
     */
    private void deleteExpiredNodes(final long t) {
        // delete entire tree
        if (root.uLimit <= t) {
            root = null;
        }
        // regulaerer Fall:
        else {
            A23TNode tempNode = root;
            while (tempNode.nodes[0] != null) {
                tempNode = tempNode.nodes[0];
            }
            if (tempNode.uLimit == t) {
                tempNode = tempNode.pNode;
                // delete left child node
                for (int i = 0; i < tempNode.nodeCount - 1; i++)
                    tempNode.nodes[i] = tempNode.nodes[i + 1];
                tempNode.nodes[tempNode.nodeCount - 1] = null;
                tempNode.nodeCount -= 1;
                // fix potential underflow
                tempNode.mergeSplit(t);
            }
        }
    }

    /**
     * Merges two partial aggregates. That is, it applies f_merge on the two
     * input partial aggregates and assigns the result as new partial aggregate
     * to the second input.
     *
     * @param src  the merge partner
     * @param dest the node to merge into
     */
    private void combineNodes(A23TNode src, A23TNode dest) {
        combineValues(src.values, dest.values);
        // Set max. tstart
        if (src.tstart > dest.tstart)
            dest.tstart = src.tstart;

        // Add value-count
        dest.valueCount += src.valueCount;
    }

    /**
     * Merges two partial aggregates.
     *
     * @param src  the source
     * @param dest the desination
     */
    @SuppressWarnings("unchecked")
    private void combineValues(Object[] src, Object[] dest) {
        for (AggregateFunctionHolder h : aggregates)
            dest[h.outputIndex] = h.aggregate.fMerge(dest[h.outputIndex],
                    src[h.outputIndex]);
    }

    @Override
    public int hashCode() {
        return partitionId.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        A23Tree other = (A23Tree) obj;
        if (partitionId == null) {
            if (other.partitionId != null)
                return false;
        } else if (!partitionId.equals(other.partitionId))
            return false;
        return true;
    }

    @Override
    public String toString() {
        return "A23Tree [partitionId=" + partitionId + ", lastUpdate=" + lastUpdate + "]";
    }

    ////////////////////////////////////////////////////////////////////
    //
    // INNER CLASS NODE
    //
    ////////////////////////////////////////////////////////////////////

    /**
     * A node of an Agg-2-3-tree to store partial aggregates with time interval
     * semantics. Inner nodes have exactly one partial aggregate as payload and
     * one time interval as key.
     */
    private class A23TNode implements Serializable {

        private static final long serialVersionUID = 1L;
        /**
         * As this is a variant of a 2-3-tree, every node has a maximum capacity
         * of 3.
         */
        private final static int MAX_CAPACITY = 3;
        /**
         * The maximum value of the key. This value is used to organize an
         * Agg-2-3-tree like a 2-3-tree.
         */
        private long uLimit;
        /**
         * The partial aggregates. There might be multiple partial aggregates
         * per payload in order to support grouping.
         */
        private Object[] values;
        private long tstart;
        /**
         * The number of partial aggregates.
         */
        private long valueCount;
        /**
         * The number of child nodes this node currently has.
         */
        private int nodeCount;
        /**
         * The parent node of this node. If it is set to NULL, then this node is
         * the root of an Agg-2-3-tree.
         */
        private A23TNode pNode;
        /**
         * The child nodes of this nodes. Child nodes are ordered by their keys.
         */
        private A23TNode[] nodes;

        /**
         * Creates a new node of the Agg-2-3-tree and initializes it with the
         * first value.
         *
         * @param event the event
         */
        @SuppressWarnings("unchecked")
        private A23TNode(Event event) {
            uLimit = event.getT2();
            tstart = event.getT1();
            values = new Object[skeleton.length];
            System.arraycopy(skeleton, 0, values, 0, skeleton.length);
            for (AggregateFunctionHolder h : aggregates)
                values[h.outputIndex] = h.aggregate.fInit(event.get(h.inputIndex));
            valueCount = 1;
            nodeCount = 0;
            pNode = null;
            nodes = new A23TNode[MAX_CAPACITY];
        }

        /**
         * Creates a new node of the Agg-2-3-tree and takes over the given
         * partial aggregate without copying the child-nodes and parent-node.
         *
         * @param node the aggregate to copy
         */
        private A23TNode(A23TNode node) {
            uLimit = node.uLimit;
            values = new Object[skeleton.length];
            System.arraycopy(node.values, 0, values, 0, skeleton.length);
            valueCount = node.valueCount;
            nodeCount = 0;
            pNode = null;
            nodes = new A23TNode[MAX_CAPACITY];
        }

        /**
         * Creates a new node of the Agg-2-3-tree and initializes it with the
         * neutral element.
         *
         * @param up maximum value of the key
         */
        private A23TNode(long up) {
            uLimit = up;
            values = new Object[skeleton.length];
            System.arraycopy(skeleton, 0, values, 0, skeleton.length);
            this.valueCount = 0;
            nodeCount = 0;
            pNode = null;
            nodes = new A23TNode[MAX_CAPACITY];
        }

        ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
        // Auxiliary operations of the Agg-2-3-tree //
        ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

        /**
         * Adds an existing node to this node in form of a new child.
         *
         * @param n node beeing added to this node
         */
        private void addNode(A23TNode n) {
            if (nodeCount < 3) { // there is free space n can be added to
                n.pNode = this; // this node becomes the parent node of n
                out:
                for (int i = 0; i <= nodeCount; i++) { // insert n such
                    // that all nodes
                    // keep ordered
                    if (i == nodeCount) {
                        nodes[i] = n;
                        break out;
                    } else if (nodes[i].uLimit > n.uLimit) {
                        for (int j = nodeCount; j > i; j--) {
                            nodes[j] = nodes[j - 1];
                        }
                        nodes[i] = n;
                        break out;
                    }
                }
                nodeCount += 1;
            } else { // there isn't enough free space
                // create two new nodes that will replace this one
                A23TNode splitNode1 = new A23TNode(0L);
                A23TNode splitNode2 = new A23TNode(uLimit); // gets the key of
                // this node

                // Distribute the four child nodes of this node such that each
                // split node gets exactly 2 of them.
                // After distribution, the key of the first split node can be
                // set.
                if (n.uLimit < nodes[0].uLimit) {
                    n.pNode = splitNode1;
                    nodes[0].pNode = splitNode1;
                    nodes[1].pNode = splitNode2;
                    nodes[2].pNode = splitNode2;
                    splitNode1.nodes[0] = n;
                    splitNode1.nodes[1] = nodes[0];
                    splitNode2.nodes[0] = nodes[1];
                    splitNode2.nodes[1] = nodes[2];
                    splitNode1.uLimit = nodes[0].uLimit;
                } else if (n.uLimit < nodes[1].uLimit) {
                    n.pNode = splitNode1;
                    nodes[0].pNode = splitNode1;
                    nodes[1].pNode = splitNode2;
                    nodes[2].pNode = splitNode2;
                    splitNode1.nodes[0] = nodes[0];
                    splitNode1.nodes[1] = n;
                    splitNode2.nodes[0] = nodes[1];
                    splitNode2.nodes[1] = nodes[2];
                    splitNode1.uLimit = n.uLimit;
                } else if (n.uLimit < nodes[2].uLimit) {
                    n.pNode = splitNode2;
                    nodes[0].pNode = splitNode1;
                    nodes[1].pNode = splitNode1;
                    nodes[2].pNode = splitNode2;
                    splitNode1.nodes[0] = nodes[0];
                    splitNode1.nodes[1] = nodes[1];
                    splitNode2.nodes[0] = n;
                    splitNode2.nodes[1] = nodes[2];
                    splitNode1.uLimit = nodes[1].uLimit;
                } else {
                    n.pNode = splitNode2;
                    nodes[0].pNode = splitNode1;
                    nodes[1].pNode = splitNode1;
                    nodes[2].pNode = splitNode2;
                    splitNode1.nodes[0] = nodes[0];
                    splitNode1.nodes[1] = nodes[1];
                    splitNode2.nodes[0] = nodes[2];
                    splitNode2.nodes[1] = n;
                    splitNode1.uLimit = nodes[1].uLimit;
                }
                splitNode1.nodeCount = (MAX_CAPACITY + 1) / 2;
                splitNode2.nodeCount = (MAX_CAPACITY + 1) / 2;
                if (pNode == null) { // this is the root node -> the
                    // Agg-2-3-tree must grow to resolve the
                    // overflow
                    A23TNode tempPNode = new A23TNode(this);
                    tempPNode.nodes[0] = splitNode1;
                    tempPNode.nodes[1] = splitNode2;
                    tempPNode.nodeCount = 2;
                    splitNode1.pNode = tempPNode;
                    splitNode2.pNode = tempPNode;
                    A23Tree.this.root = tempPNode;
                } else { // propagate
                    combineNodes(this, splitNode1);
                    combineNodes(this, splitNode2);
                    pNode.deleteNode(this);
                    pNode.addNode(splitNode1);
                    pNode.addNode(splitNode2);
                }
            }
        }

        /**
         * Merges the partial aggregates of this node with all its child nodes.
         */
        private void reduce() {
            // merge with each child
            for (int i = 0; i < nodeCount; i++)
                combineNodes(this, nodes[i]);
            // set the partial aggregate of this node to the neutral element e
            this.valueCount = 0;
            System.arraycopy(skeleton, 0, values, 0, skeleton.length);
        }

        ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
        // Help Methods //
        ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

        /**
         * Simply deletes a node from this node, if it exists.
         *
         * @param node node to delete
         */
        private void deleteNode(A23TNode node) {
            for (int i = 0; i < nodeCount; i++) {
                if (nodes[i].equals(node)) {
                    if (i == nodeCount - 1) {
                        nodes[i] = null;
                        nodeCount -= 1;
                        return;
                    } else {
                        for (int j = i; j < nodeCount - 1; j++) {
                            nodes[j] = nodes[j + 1];
                            nodes[j + 1] = null;
                        }
                        nodeCount -= 1;
                        return;
                    }
                }
            }
            nodeCount -= 1;
        }

        /**
         * This method is used to push down a new node among the rightmost path.
         * This must be done whenever a new node's key is greater that the key
         * of the root.
         *
         * @param node new node being inserted
         */
        private void appendNode(A23TNode node) {
            A23TNode tempNode = this;
            // merge all partial aggregates among te entire rightmost path
            while (tempNode.nodeCount > 0) {
                tempNode.reduce();
                tempNode = tempNode.nodes[tempNode.nodeCount - 1];
            }
            // Add node as a new leaf node but with neutral partial aggregate
            tempNode.pNode.addNode(new A23TNode(node.uLimit));
            // Update keys and traverse up
            while (tempNode.pNode != null) {
                tempNode.pNode.uLimit = node.uLimit;
                tempNode = tempNode.pNode;
            }
            // Add the new partial aggregate to the root node (i.e., the entire
            // tree)
            combineNodes(node, tempNode);
        }

        /**
         * This method is required to insert a new node into a node that is (i)
         * not a leaf node (ii) covers the node being inserted completely
         *
         * @param node node being inserted
         */
        private void insertNode(A23TNode node) {
            for (int i = 0; i < nodeCount; i++) {
                if (nodes[i].uLimit < node.uLimit)
                    combineNodes(node, nodes[i]);
                else if (node.uLimit == nodes[i].uLimit) {
                    combineNodes(node, nodes[i]);
                    return;
                }
                // else if (node.uLimit < nodes[i].uLimit && nodeCount > 0) {
                else if (nodes[i].nodeCount > 0) {
                    nodes[i].insertNode(node);
                    return;
                }
                // else if (node.uLimit < nodes[i].uLimit && nodeCount == 0){
                else if (nodes[i].nodeCount == 0) {
                    combineNodes(nodes[i], node);
                    addNode(node);
                    return;
                }
            }
        }

        /**
         * Fixes potential underflow after node deletion.
         *
         * @param t the timestamp
         */
        private void mergeSplit(long t) {
            A23TNode tempNode = this;
            if (tempNode.nodeCount == 1 && tempNode.pNode != null) {
                A23TNode nNode = tempNode.pNode.nodes[1];
                if (tempNode.valueCount > 0)
                    tempNode.reduce();
                if (nNode.valueCount > 0)
                    nNode.reduce();
                if (nNode.nodeCount == MAX_CAPACITY) { // there is a middle
                    // child node
                    tempNode.addNode(nNode.nodes[0]);
                    for (int j = 0; j < nNode.nodeCount - 1; j++)
                        nNode.nodes[j] = nNode.nodes[j + 1];
                    nNode.nodes[2] = null;
                    nNode.nodeCount -= 1;
                    tempNode.uLimit = tempNode.nodes[1].uLimit;
                } else { // there is no middle child node
                    nNode.addNode(tempNode.nodes[0]);
                    nNode.pNode.deleteNode(tempNode);
                    nNode.pNode.mergeSplit(t);
                }
            } else if (tempNode.nodeCount == 1 && tempNode.pNode == null) { // necessary
                // for
                // handling
                // underflow
                // in
                // root
                // node
                // push down partial aggregate from root to right child node
                combineNodes(this, nodes[0]);
                // declare right child node as new root node
                nodes[0].pNode = null;
                A23Tree.this.root = nodes[0];
            }
        }

        /**
         * Creates the output event for the leftmost partial aggregate. Should
         * only be called on the leftmost leaf node.
         *
         * @param t the timestamp
         */
        @SuppressWarnings("unchecked")
        private void lookUp(long t) {

            A23TNode tempNode = this;
            // TODO: Always values in left leaf?
            while (tempNode.valueCount == 0)
                tempNode = tempNode.pNode;

            // output event
            Object[] result = new Object[skeleton.length];
            long tstart = tempNode.tstart;

            System.arraycopy(tempNode.values, 0, result, 0, skeleton.length);
            // counter needed to compute final AVG aggregates
            long resultCount = tempNode.valueCount;

            tempNode = tempNode.pNode;
            while (tempNode != null) {
                if (tempNode.valueCount > 0) { // there are partial aggregates
                    combineValues(tempNode.values, result);
                    resultCount += tempNode.valueCount;
                }
                tempNode = tempNode.pNode;
            }

            // Apply fEval
            for (AggregateFunctionHolder h : aggregates)
                result[h.outputIndex] = h.aggregate.fEval(result[h.outputIndex], resultCount);

            // set timestamps of output event
            if (lastUpdate > tstart)
                tstart = lastUpdate;

            lastUpdate = t;
            // output new event
            callback.receive(new SimpleEvent(result, tstart, t));
        }

        @Override
        public String toString() {
            return "A23TNode [uLimit=" + uLimit + ", valueCount=" + valueCount + ", nodeCount=" + nodeCount + "]";
        }
    }
}
