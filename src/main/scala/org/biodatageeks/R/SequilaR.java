package org.biodatageeks.R;

import org.apache.spark.sql.SparkSession;
import org.biodatageeks.utils.SequilaRegister;

public class SequilaR {
    private static SequilaR ourInstance = new SequilaR();

    public static SequilaR getInstance() {
        return ourInstance;
    }

    private SequilaR() {
    }

    public static void init(SparkSession spark) {

        SequilaRegister.register(spark);

    }
}