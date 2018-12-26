Superset
========

.. contents::

Running using docker-compose
############################

.. code-block:: bash

    git clone https://github.com/ZSI-Bio/bdg-sequila.git
    cd bdg-sequila/analytics_platform/sequila
    ./start.sh --master=local[2] --driver-memory=2g --data-dir=/data/input/bams --sequila-version=|version| --superset-version=0.28.1

Params:

- ``master``- Apache Spark master to use, e.g. local[1]
- ``diver-memory`` - Apache Spark driver memory, e.g. 4g
- ``data-dir`` - a directory on the host containing alignment files used for table-mapping , e.g. /data/input/bams
- ``sequila-version`` - SeQuiLa image version to use, e.g. |version|
- ``superset-version`` - Apache Superset image version to use, e.g. 0.28.1

