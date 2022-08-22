package org.biodatageeks.sequila.rangejoins.exp.ailist;

import org.biodatageeks.sequila.rangejoins.methods.base.BaseIntervalHolder;
import org.biodatageeks.sequila.rangejoins.methods.base.BaseNode;
import scala.Option;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.Objects;

// Augmented Interval List implementation
// paper: https://www.biorxiv.org/content/10.1101/593657v1
// Followed:
//     paper repo - C implementation: https://github.com/databio/AIList
//     another public repo - Rust implementation: https://github.com/sstadick/ScAIList
public class AIList<T> implements BaseIntervalHolder<T> {
	// the list of intervals
	private ArrayList<Interval<T>> mIntervals;
	// number of components in total
	private int mNumComps;
	// list of sizes of components
	private final ArrayList<Integer> mCompSizes = new ArrayList<>();
	// list of components' starting indices
	private final ArrayList<Integer> mCompIds = new ArrayList<>();
	// list of maximal end values
	// paired with intervals by indices
	private final ArrayList<Integer> mMaxEnds = new ArrayList<>();

	protected void sortIntervals() {
		// todo Radix sort implementation
		mIntervals.sort(Comparator.comparingInt(iv -> iv.mStart));
	}

	protected void decomposeList(
			final int maxComps,
			final int minCovLen,
			final int minCov,
			final int minCompSize
	) {
		if (mIntervals.size() <= minCompSize) {
			mNumComps = 1;
			mCompSizes.add(mIntervals.size());
			mCompIds.add(0);
			return;
		}
		ArrayList<Interval<T>> decomposed = new ArrayList<>();
		final int inputSize = mIntervals.size();

		// decompose while
		// max component number is not exceeded
		// and number of intervals left is big enough
		// to be worth decomposing it
		while (mNumComps < maxComps && inputSize - decomposed.size() > minCompSize) {
			// list2 contains extracted intervals
			// list1 contains all intervals left
			ArrayList<Interval<T>> list1 = new ArrayList<>();
			ArrayList<Interval<T>> list2 = new ArrayList<>();
			for (int i = 0; i < mIntervals.size(); i++) {
				final Interval<T> interval = mIntervals.get(i);
				int j = 1;
				int cov = 0;
				// count intervals covered by i'th interval
				while (j <= minCovLen && cov < minCov && i + j < mIntervals.size()) {
					if (mIntervals.get(i + j).mEnd <= interval.mEnd) {
						cov++;
					}
					j++;
				}
				// check if it is worth to extract i'th interval
				// and if it is do so
				if (cov < minCov) {
					list1.add(interval);
				} else {
					list2.add(interval);
				}
			}
			// add the component info
			mCompIds.add(decomposed.size());
			mCompSizes.add(list1.size());
			mNumComps++;

			// check if list2 will be the last component
			if (list2.size() <= minCompSize || mNumComps == maxComps - 2) {
				// no more decomposing, add list2 if not empty
				// if it is empty, then list1 was already added
				// in the previous loop
				if (!list2.isEmpty()) {
					decomposed.addAll(list1);
					mCompIds.add(decomposed.size());
					mCompSizes.add(list2.size());
					decomposed.addAll(list2);
					mNumComps++;
				} else if (list1.size() > minCompSize) {
					// if last component has size > minCompSize
					// but can't be decomposed
					decomposed.addAll(list1);
				}
			} else {
				// prepare for the next loop
				decomposed.addAll(list1);
				mIntervals = list2;
			}
		}
		mIntervals = decomposed;
	}

	protected void augmentWithMaxEnds() {
		for (int i = 0; i < mNumComps; i++) {
			final int compStart = mCompIds.get(i);
			final int compEnd = compStart + mCompSizes.get(i);
			int maxEnd = mIntervals.get(compStart).mEnd;
			mMaxEnds.add(maxEnd);
			for (int j = compStart+1; j < compEnd; j++) {
				final int intervalEnd = mIntervals.get(j).mEnd;
				if (intervalEnd > maxEnd) {
					maxEnd = intervalEnd;
				}
				mMaxEnds.add(maxEnd);
			}
		}
	}

	public AIList() {
		mIntervals = new ArrayList<>();
	}

	// note that this implementation of put doesn't return
	// previous value of provided interval
	@Override
	public T put(int start, int end, T value) {
		mIntervals.add(new Interval<>(start, end, value));
		return null;
	}

	@Override
	public T remove(int start, int end) {
		return null;
	}

	@Override
	public BaseNode<T> find(int start, int end) {
		return null;
	}

	@Override
	public Iterator<BaseNode<T>> overlappers(int start, int end) {
		return new OverlapIterator(start, end, this);
	}

	@Override
	public void postConstruct(Option<Object> domains) {
		sortIntervals();

		// max number of components
		final int maxComps = 10;
		// number of elements ahead to check for coverage
		final int minCovLen = 20;
		// number of elements covered to trigger an extraction
		final int minCov = minCovLen / 2;
		// min size of a single component
		final int minCompSize = 64;
		decomposeList(maxComps, minCovLen, minCov, minCompSize);

		augmentWithMaxEnds();
	}
	@Override
	public Iterator<BaseNode<T>> iterator() {
		return null;
	}

	public ArrayList<Interval<T>>getIntervals() {
		return mIntervals;
	}

	public int getNumComps() {
		return mNumComps;
	}

	public ArrayList<Integer> getCompSizes() {
		return mCompSizes;
	}

	public ArrayList<Integer> getCompIds() {
		return mCompIds;
	}

	public ArrayList<Integer> getMaxEnds() {
		return mMaxEnds;
	}

	public static class Interval<T1> extends BaseNode<T1> implements Serializable {
		private int mStart;
		private final int mEnd;
		private final ArrayList<T1> mValue = new ArrayList<>();

		Interval(final int start, final int end, final T1 value) {
			mStart = start;
			mEnd = end;
			mValue.add(value);
		}

		@Override
		public String toString() {
			return "Interval{" +
					"mStart=" + mStart +
					", mEnd=" + mEnd +
					", mValue=" + mValue +
					'}';
		}

		@Override
		public int getStart() {
			return mStart;
		}

		public void setStart(int start) {
			mStart = start;
		}

		@Override
		public int getEnd() {
			return mEnd;
		}

		@Override
		public ArrayList<T1> getValue() {
			return mValue;
		}

		@Override
		public boolean equals(Object o) {
			if (this == o) return true;
			if (o == null || getClass() != o.getClass()) return false;
			Interval<?> interval = (Interval<?>) o;
			return mStart == interval.mStart && mEnd == interval.mEnd && mValue.equals(interval.mValue);
		}

		@Override
		public int hashCode() {
			return Objects.hash(mStart, mEnd, mValue);
		}
	}

	protected static <T1> int rightMost(
			final int qEnd,
			final ArrayList<Interval<T1>> intervals,
			int lo,
			int hi)
	{
		if (intervals.get(hi - 1).mStart <= qEnd) {
			// last start is less than the qEnd, then return the last start pos
			return hi - 1;
		} else if (intervals.get(lo).mStart > qEnd) {
			// first start > qEnd, not in this component at all
			return -1;
		}
		int mid;
		while (lo < hi) {
			mid = lo + (hi - lo) / 2;
			if (intervals.get(mid).mStart <= qEnd) {
				lo = mid + 1;
			} else {
				hi = mid;
			}
		}
		// we need r.start < q.end
		if (intervals.get(lo).mStart >= qEnd) {
			return lo - 1;
		}
		// not sure if it's reachable
		return lo;
	}

	public static class OverlapIterator<T1> implements Iterator<BaseNode<T1>>
	{
		private final int mQStart;
		private final int mQEnd;
		private final AIList<T1> mAIListRef;
		private int mCurCompIdx;
		private boolean mFindIvIdx;
		private int mCurIvIdx = 0;
		private boolean mBreakFromWhile = false;
		private Interval<T1> mNext;

		public OverlapIterator(final int start, final int end, final AIList<T1> aiListRef)
		{
			mQStart = start;
			mQEnd = end;
			mAIListRef = aiListRef;
			mCurCompIdx = 0;
			// Flag to determine whether we should start
			// searching in a new component or stay in current one.
			mFindIvIdx = true;
			mCurIvIdx = 0;
			mBreakFromWhile = false;
			mNext = getNextInterval();
		}

		@Override
		public boolean hasNext() {
			return mNext != null;
		}

		@Override
		public Interval<T1> next() {
			Interval<T1> tmp = mNext;
			mNext = getNextInterval();
			return tmp;
		}

		public Interval<T1> getNextInterval()
		{
			// Iterate over all components
			while (mCurCompIdx < mAIListRef.getNumComps()) {
				final int compStart = mAIListRef.mCompIds.get(mCurCompIdx);
				final int compEnd = compStart + mAIListRef.mCompSizes.get(mCurCompIdx);
				// If component has more than 15 intervals then use binary search
				// to get right most interval. Otherwise perform linear search.
				if (mAIListRef.mCompSizes.get(mCurCompIdx) > 15) {
					if (mFindIvIdx) {
						mCurIvIdx = rightMost(
								mQEnd, mAIListRef.mIntervals,
								compStart, compEnd
						);
						if (mCurIvIdx < 0) {
							mCurCompIdx++;
							mFindIvIdx = true;
							mCurIvIdx = 0;
							continue;
						}
						mFindIvIdx = false;
					}
					// Starting from found right most interval return all overlapping intervals until:
					//  we step outside of current component
					//  max end value is greater than start value of query interval
					//  mBreakFromWhile flag is set.
					while (mCurIvIdx >= compStart
							&& mAIListRef.mMaxEnds.get(mCurIvIdx) >= mQStart
							&& !mBreakFromWhile)
					{
						final Interval<T1> interval = mAIListRef.mIntervals.get(mCurIvIdx);
						mCurIvIdx--;
						if (mCurIvIdx < 0) {
							mBreakFromWhile = true;
							mCurIvIdx = 0;
						}
						if (interval.mEnd >= mQStart) {
							return interval;
						}
					}
				} else {
					if (mCurIvIdx < compStart) {
						mCurIvIdx = compStart;
					}
					while (mCurIvIdx < compEnd) {
						final Interval<T1> interval = mAIListRef.mIntervals.get(mCurIvIdx);
						mCurIvIdx++;
						if (interval.mStart <= mQEnd && interval.mEnd >= mQStart) {
							return interval;
						}
					}
				}
				mBreakFromWhile = false;
				mFindIvIdx = true;
				mCurCompIdx++;
			}
			return null;
		}
	}
}
