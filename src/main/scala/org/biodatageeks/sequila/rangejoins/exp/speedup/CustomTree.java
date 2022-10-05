package org.biodatageeks.sequila.rangejoins.exp.speedup;

import org.biodatageeks.sequila.rangejoins.methods.base.BaseIntervalHolder;
import org.biodatageeks.sequila.rangejoins.methods.base.BaseNode;
import org.jetbrains.annotations.NotNull;
import scala.Option;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;

public class CustomTree<V> implements BaseIntervalHolder<V> {

    public final ArrayList<CustomNode<V>> intervals = new ArrayList<>();
    private int K;
    private int rootIndex;

    @Override
    public V put(int start, int end, V value) {
        intervals.add(new CustomNode<>(start, end, value));
        return value;
    }

    @Override
    public V remove(int start, int end) {
        return null;
    }

    @Override
    public BaseNode<V> find(int start, int end) {
        return null;
    }

    @Override
    public Iterator<BaseNode<V>> overlappers(int start, int end) {
        return new CustomIterator(start, end);
    }

    void augmentTree() {
        recurAugmentMax(rootIndex, K);
        int i = 0;
        int runningMax = intervals.get(0).getEnd();
        intervals.get(0).leftMaxEnd = runningMax;
        i++;
        while (i < intervals.size()) {
            if (runningMax < intervals.get(i - 1).getEnd()) {
                runningMax = intervals.get(i - 1).getEnd();
            }
            intervals.get(i).leftMaxEnd = runningMax;
            i++;
        }
    }

    int recurAugmentMax(int nodeIndex, int level) {
        if (level == 0 && nodeIndex <= intervals.size() - 1) {
            // regular leaf
            intervals.get(nodeIndex).max = intervals.get(nodeIndex).getEnd();
            return intervals.get(nodeIndex).max;
        } else if (level == 0 && nodeIndex > intervals.size() - 1) {
            // imaginary leaf
            return -1;
        } else if (level > 0 && nodeIndex <= intervals.size() - 1) {
            // only right can be imaginary here but can return some value
            int leftSubtreeMax = recurAugmentMax(nodeIndex - (int) Math.pow(2, level - 1), level - 1);
            int rightSubtreeMax = recurAugmentMax(nodeIndex + (int) Math.pow(2, level - 1), level - 1);
            intervals.get(nodeIndex).max = Math.max(Math.max(leftSubtreeMax, rightSubtreeMax), intervals.get(nodeIndex).getEnd());
            return intervals.get(nodeIndex).max;
        } else if (level > 0 && nodeIndex > intervals.size() - 1) {
            // imaginary node, it makes sens to analyze only left subtree cos right is always imaginary
            return recurAugmentMax(nodeIndex - (int) Math.pow(2, level - 1), level - 1);
        }
        throw new RuntimeException("Fix implementation");
    }

    @Override
    public void postConstruct(Option<Object> domains) {
        intervals.sort(new CustomNodeComparator());
        TreeMetadata metadata = MetadataResolver.resolveMetadata(intervals.size());
        K = metadata.K;
        rootIndex = metadata.rootIndex;
        augmentTree();
    }

    @NotNull
    @Override
    public Iterator<BaseNode<V>> iterator() {
        return null;
    }

    public class CustomIterator implements Iterator<BaseNode<V>> {

        final private int qStart;
        final private int qEnd;
        private int qNext;
        private final int firstOverlap;

        public CustomIterator(int qStart, int qEnd) {
            this.qStart = qStart;
            this.qEnd = qEnd;
            this.qNext = firstOverlap(rootIndex, K);
            this.firstOverlap = this.qNext;
        }

        @Override
        public boolean hasNext() {
            return qNext != -1;
        }

        @Override
        public CustomNode<V> next() {
            int previous = qNext;
            qNext = nextOverlap();
            return intervals.get(previous);
        }
        private int nextOverlap() {
            int currentNodeIndex = qNext;
            boolean finishedRight = false;
            while (true) {
                // we're on left, after processing right side
                if (currentNodeIndex < firstOverlap) {
                    if ((currentNodeIndex == firstOverlap-1) && finishedRight) {
                        currentNodeIndex++;
                    }
                    while (stillPossibleOnLeftOverlap(currentNodeIndex)) {
                        currentNodeIndex--;
                        if (intervals.get(currentNodeIndex).overlaps(qStart, qEnd)) {
                            return currentNodeIndex;
                        }
                    }
                    return -1;
                } else {
                    currentNodeIndex++;
                    if (!isImaginary(currentNodeIndex)) {
                        if (intervals.get(currentNodeIndex).overlaps(qStart, qEnd)) {
                            return currentNodeIndex;
                        }
                        if (!stillPossibleOnRightOverlap(currentNodeIndex)) {
                            currentNodeIndex = firstOverlap - 1;
                            finishedRight = true;
                        }
                    } else {
                        currentNodeIndex = firstOverlap - 1;
                        finishedRight = true;
                    }
                }
            }
        }

        private boolean stillPossibleOnLeftOverlap(int nodeIndex) {
            return nodeIndex > 0 && intervals.get(nodeIndex).leftMaxEnd >= qStart;
        }

        private boolean stillPossibleOnRightOverlap(int nodeIndex) {
            return intervals.get(nodeIndex).getStart() <= qEnd;
        }


        private boolean isImaginary(int nodeIndex) {
            return nodeIndex > intervals.size() - 1;
        }

        private int firstOverlap(int from, int fromLevel) {
            int nodeIndex = from;
            int result = -1;
            if (intervals.get(nodeIndex).max < qStart) {
                return -1;
            }
            while (true) {
                if (intervals.get(nodeIndex).overlaps(qStart, qEnd)) {
                    return nodeIndex;
                } else {
                    int left = TreeOperations.left(nodeIndex, fromLevel);
                    if (left != -1 && intervals.get(left).max >= qStart) {
                        nodeIndex = left;
                        fromLevel--;
                    } else {
                        if (intervals.get(nodeIndex).getStart() > qEnd) {
                            break;
                        }
                        nodeIndex = TreeOperations.right(nodeIndex, fromLevel);
                        fromLevel--;

                        if (fromLevel == -1) {
                            break;
                        }

                        while (isImaginary(nodeIndex)) {
                            nodeIndex = TreeOperations.left(nodeIndex, fromLevel);
                            fromLevel--;
                        }

                        if (intervals.get(nodeIndex).max < qStart) {
                            break;
                        }
                    }
                }
            }
            return result;
        }

    }

    public static class CustomNode<V> extends BaseNode<V> {
        private int start;
        private int end;

        int leftMaxEnd = 0;

        int max = -1;
        private ArrayList<V> values = new ArrayList<>(1);

        @Override
        public int getStart() {
            return start;
        }

        @Override
        public int getEnd() {
            return end;
        }

        @Override
        public ArrayList<V> getValue() {
            return values;
        }

        public CustomNode(int start, int end, V value) {
            this.start = start;
            this.end = end;
            this.values.add(value);
        }

        boolean overlaps(final int qStart, final int qEnd) {
            return this.start <= qEnd && this.end >= qStart;
        }

    }

    public class CustomNodeComparator implements Comparator<CustomNode<V>> {
        @Override
        public int compare(CustomNode o1, CustomNode o2) {
            int result = 0;

            if (o1.getStart() > o2.getStart())
                result = 1;
            else if (o1.getStart() < o2.getStart())
                result = -1;
            else if (o1.getEnd() > o2.getEnd())
                result = 1;
            else if (o1.getEnd() < o2.getEnd())
                result = -1;
            return result;
        }
    }
}
