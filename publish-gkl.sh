#!/usr/bin/env bash
curl -v --user 'admin:changeMe' --upload-file ./lib/gkl-0.8.5-1-darwin-SNAPSHOT.pom http://zsibio.ii.pw.edu.pl/nexus/repository/maven-snapshots/com/intel/gkl/gkl/0.8.5-1-darwin-SNAPSHOT/
curl -v --user 'admin:changeMe' --upload-file ./lib/gkl-0.8.5-1-darwin-SNAPSHOT.jar http://zsibio.ii.pw.edu.pl/nexus/repository/maven-snapshots/com/intel/gkl/gkl/0.8.5-1-darwin-SNAPSHOT/


curl -v --user 'admin:changeMe' --upload-file ./lib/gkl-0.8.5-1-linux-SNAPSHOT.pom http://zsibio.ii.pw.edu.pl/nexus/repository/maven-snapshots/com/intel/gkl/gkl/0.8.5-1-linux-SNAPSHOT/
curl -v --user 'admin:changeMe' --upload-file ./lib/gkl-0.8.5-1-linux-SNAPSHOT.jar http://zsibio.ii.pw.edu.pl/nexus/repository/maven-snapshots/com/intel/gkl/gkl/0.8.5-1-linux-SNAPSHOT/
