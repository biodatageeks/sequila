package htsjdk.samtools;

public class SAMRecordHelper {
    public static void eagerDecode(SAMRecord record) {
        record.eagerDecode();
    }
}
