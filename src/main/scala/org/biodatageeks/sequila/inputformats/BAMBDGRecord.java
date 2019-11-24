package htsjdk.samtools;


public class BAMBDGRecord extends BAMRecord {

    private byte[] mRestOfBinaryData = null;
    protected BAMBDGRecord(final SAMFileHeader header,
                        final int referenceID,
                        final int coordinate,
                        final short readNameLength,
                        final short mappingQuality,
                        final int indexingBin,
                        final int cigarLen,
                        final int flags,
                        final int readLen,
                        final int mateReferenceID,
                        final int mateCoordinate,
                        final int insertSize,
                        final byte[] restOfData) {
        super(header,referenceID ,coordinate, readNameLength, mappingQuality, indexingBin, cigarLen, flags, readLen, mateReferenceID,mateCoordinate,insertSize,restOfData );

    }

    @Override
    protected void eagerDecode() {
        getReadName();
        getCigar();
        getReadBases();
        getBaseQualities();
        //getBinaryAttributes();
        super.eagerDecode();
        mRestOfBinaryData = null;
    }

}
