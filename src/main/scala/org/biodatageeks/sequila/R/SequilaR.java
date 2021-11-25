package org.biodatageeks.sequila.R;

import org.apache.spark.SparkContext;
import org.apache.spark.sql.SequilaSession;
import org.apache.spark.sql.SparkSession;
import org.biodatageeks.sequila.utils.UDFRegister;

public class SequilaR {
    private static SequilaR ourInstance = new SequilaR();

    public static SequilaR getInstance() {
        return ourInstance;
    }

    private SequilaR() {
    }

    public static SequilaSession init(){

        SparkSession spark = SparkSession.builder().getOrCreate();
        SequilaSession ss = new SequilaSession(spark);
        UDFRegister.register(ss);
        return ss;

    }

    public static Boolean dropTempView(SequilaSession ss,String tableName ){
       return  ss.catalog().dropTempView(tableName);

    }
}