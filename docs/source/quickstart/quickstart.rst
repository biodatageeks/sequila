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


In your command line pull docker image from Docker Hub and invoke smoketests

.. code-block:: bash

   docker pull biodatageeks/bdg-toolset

   docker run -e USERID=$UID -e GROUPID=$(id -g) \
     biodatageeks/bdg-toolset \
     bdg-shell -i /tmp/smoketest.scala



This will open bdg-shell (wrapper for spark-shell) and load ``smoketest.scala`` code for execution. Since all test data are already packaged and accessible within the container you may just wait for results. Which won't take too long. (Estimated time ~ 1 minute)

At the end you should see the following output:

.. image:: quick_start_check.*

From the screenshot above you can see that our optimized IntervalTree-based join strategy was used. Some additional debug information were logged to the console.

The final result should be as follows:
::

   TEST PASSED


Congratulations! Your installation is working on sample data.

.. note::

   If you are wondering what this part ``-e USERID=$UID -e GROUPID=$(id -g)``  is for: It allows the docker container's inside-user to write in mounted volumes with host's user id and group id.  






