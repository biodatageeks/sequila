package org.biodatageeks.sequila.rangejoins.exp.speedup;


public class TreeOperations {
	static public int left(final int arrayIndex, final int level) {
		if (level > 0) {
			return arrayIndex - (1 << (level - 1));
		} else {
			return -1;
		}
	}

	// get node's right child, or invalidIndex if called on a leaf
	static public int right(final int arrayIndex, final int level) {
		if (level > 0) {
			return arrayIndex + (1 << (level - 1));
		} else {
			return -1;
		}
	}


	static public int parent(int arrayIndex, int level) {
		int ofs = 1 << level;
		if (isRightChild(arrayIndex, level)) { // node is right child
			return arrayIndex - ofs;
		}
		// node is a left child
		return arrayIndex  + ofs;
	}

	static public int leftMostLeaf(int arrayIndex, int level) {
		int ofs = (1 << level) - 1;
		return arrayIndex - ofs;
	}

	static public boolean isRightChild(int arrayIndex, int level) {
		return ((arrayIndex >> (level + 1)) & 1) != 0;
	}
}
