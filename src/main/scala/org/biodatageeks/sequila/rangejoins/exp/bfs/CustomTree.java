package org.biodatageeks.sequila.rangejoins.exp.bfs;


import org.biodatageeks.sequila.rangejoins.methods.base.BaseIntervalHolder;
import org.biodatageeks.sequila.rangejoins.methods.base.BaseNode;
import org.jetbrains.annotations.NotNull;
import scala.Option;

import java.util.*;

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
		private int qNext = -1;
		int globalLevel = K;
		Queue<Integer> queue = new ArrayDeque<>(12);
		int maxQueueSize = -1;

		public CustomIterator(int qStart, int qEnd) {
			this.qStart = qStart;
			this.qEnd = qEnd;
			queue.add(rootIndex);
			queue.add(-2);
			nextOverlap();
		}

		@Override
		public boolean hasNext() {
			return qNext != -1;
		}

		@Override
		public CustomNode<V> next() {
			int previous = qNext;
			nextOverlap();
			return intervals.get(previous);
		}

		private void nextOverlap() {
			final int levelEnd = -2;
			while (true) {
				if (queue.size() > maxQueueSize) {
					maxQueueSize = queue.size();
				}
				int currentNodeIndex = queue.remove();
				if (currentNodeIndex == levelEnd) {
					globalLevel--;
					queue.add(levelEnd);
					currentNodeIndex = queue.remove();
					// end of processing
					if (currentNodeIndex == levelEnd) {
						qNext = -1;
						return;
					}
				}

				if (!isImaginary(currentNodeIndex)) {
					boolean foundOverlap = false;
					if (intervals.get(currentNodeIndex).overlaps(qStart, qEnd)) {
						qNext = currentNodeIndex;
						foundOverlap = true;
					}

					if (globalLevel == 0 && foundOverlap) {
						return;
					}

					int leftChildIndex = TreeOperations.left(currentNodeIndex, globalLevel);
					int rightChildIndex = TreeOperations.right(currentNodeIndex, globalLevel);

					if (globalLevel == 1) {
						if (intervals.get(leftChildIndex).overlaps(qStart, qEnd)) {
							queue.add(leftChildIndex);
						}
						if (!isImaginary(rightChildIndex) && intervals.get(rightChildIndex).overlaps(qStart, qEnd)) {
							queue.add(rightChildIndex);
						}

						if (foundOverlap) {
							return;
						} else {
							continue;
						}
					}

					if (possibleOverlapInLeftSubtree(currentNodeIndex, leftChildIndex)) {
						queue.add(leftChildIndex);
					}

					if (possibleOverlapInRightSubtree(currentNodeIndex, rightChildIndex)) {
						queue.add(rightChildIndex);
					}
					if (foundOverlap) {
						return;
					}
				} else {
					queue.add(TreeOperations.left(currentNodeIndex, globalLevel));
				}
			}
		}

		private boolean possibleOverlapInRightSubtree(int currentNodeIndex, int rightChildIndex) {
			CustomNode<V> leftMost = intervals.get(TreeOperations.leftMostLeaf(rightChildIndex, globalLevel - 1));
			if (!isImaginary(rightChildIndex)) {
				return leftMost.start <= qEnd && intervals.get(rightChildIndex).max >= qStart;
			} else {
				return leftMost.start <= qEnd && intervals.get(currentNodeIndex).max >= qStart;
			}
		}

		private boolean possibleOverlapInLeftSubtree(int currentNodeIndex, int leftChildIndex) {
			CustomNode<V> leftMost = intervals.get(TreeOperations.leftMostLeaf(currentNodeIndex, globalLevel));
			return leftMost.start <= qEnd && intervals.get(leftChildIndex).max >= qStart;
		}

		private boolean isImaginary(int nodeIndex) {
			return nodeIndex > intervals.size() - 1;
		}
	}

	public static class CustomNode<V> extends BaseNode<V> {
		private int start;
		private int end;
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

