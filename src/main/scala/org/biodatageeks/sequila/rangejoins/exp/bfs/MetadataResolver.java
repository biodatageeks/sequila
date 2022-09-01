package org.biodatageeks.sequila.rangejoins.exp.bfs;

public class MetadataResolver {
	public static TreeMetadata resolveMetadata(final int numberOfElems) {
		int levels = 0;
		int counter = 1;
		while (counter <= numberOfElems) {
			counter *= 2;
			levels += 1;
		}
		return new TreeMetadata(levels - 1, counter - 1);
	}
}

class TreeMetadata {
	int K;
	int arraySizeIncludingImaginaryNodes;
	int rootIndex;

	public TreeMetadata(int k, int arraySizeIncludingImaginaryNodes) {
		K = k;
		rootIndex = (1 << K) - 1;
		this.arraySizeIncludingImaginaryNodes = arraySizeIncludingImaginaryNodes;
	}
}
