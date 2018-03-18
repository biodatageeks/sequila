 .. sectnum::
     :start: 1

Quickstart 
==========




Test run at local machine
#########################

Sequila is distributed as Docker image. It is available at DockerHub. 

The quickest way to test sequila is to run test example on sample data which are already packaged in docker container.

.. note::

   Prerequisities: You should have docker daemon installed and running. `<https://docs.docker.com/install/>`_


In your command line pull docker image from Docker Hub:

``docker pull biodatageeks/bdg-toolset``

Afterwards invoke:

``docker run --rm biodatageeks/bdg-toolset bdg-shell -i /tmp/unittest.scala``


This will open bdg-shell (wrapper for spark-shell) and load ``unittest.scala`` code for execution. Since all test data are already packaged and accessible within the container you may just wait for results. Which won't take too long. (Estimated time ~ 1 minute)

At the end you should see output of unit test. 

::

   TEST PASSED


Congratulations! Your installation is working on sample data.




