from pyspark.sql import SparkSession
from typeguard import check_argument_types


def register(session: SparkSession):
    """
    register(session)

    Register SQL extensions for a Spark session.

    :param session: Spark session
    """
    assert check_argument_types()
    sparkSession = session._jvm.org.apache.spark.sql.SparkSession.builder().enableHiveSupport().getOrCreate()
    ss = session._jvm.org.apache.spark.sql.SequilaSession(sparkSession)
    session._jvm.org.biodatageeks.utils.SequilaRegister.register(ss)
    session._jvm.org.biodatageeks.utils.UDFRegister.register(ss)
    return


class  SequilaSession (SparkSession):
    def __init__(self, session: SparkSession, jsparkSession=None):
        """Creates a new SequilaSession.

        """
        ss = session._jvm.org.apache.spark.sql.SequilaSession(session._jsparkSession)
        session._jvm.org.biodatageeks.utils.SequilaRegister.register(ss)
        session._jvm.org.biodatageeks.utils.UDFRegister.register(ss)
        session._jvm.SequilaSession.setDefaultSession(ss)
        sequilaSession = SequilaSession._instantiatedSession
        from pyspark.sql.context import SQLContext
        self._sc = sequilaSession._sc
        self._jsc = self._sc._jsc
        self._jvm = session._jvm
        if jsparkSession is None:
            if self._jvm.SequilaSession.getDefaultSession().isDefined() \
                    and not self._jvm.SequilaSession.getDefaultSession().get() \
                    .sparkContext().isStopped():
                jsparkSession = self._jvm.SequilaSession.getDefaultSession().get()
            else:
                jsparkSession = self._jvm.SequilaSession(self._jsc.sc())
        self._jsparkSession = jsparkSession
        self._jwrapped = self._jsparkSession.sqlContext()
        self._wrapped = SQLContext(self._sc, self, self._jwrapped)
        if SequilaSession._instantiatedSession is None \
                or SequilaSession._instantiatedSession._sc._jsc is None:
            SequilaSession._instantiatedSession = self
            self._jvm.SparkSession.setDefaultSession(self._jsparkSession)
