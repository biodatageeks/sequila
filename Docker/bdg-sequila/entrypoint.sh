#!/bin/bash

# Add local user
# Either use the LOCAL_USER_ID if passed in at runtime or
# fallback


#echo "Starting with UID : $USERID"
if [ -z $(grep ":$GROUPID:" /etc/group) ]; then groupadd -g $GROUPID bdgeekgroup; fi
GROUPNAME=`grep :$GROUPID: /etc/group | cut -f1 -d':'`
useradd --shell /bin/bash -u $USERID -g $GROUPNAME -o -c "" -m bdgeek
rm -rf /home/bdgeek
mv /home/tempuser /home/bdgeek
export HOME=/home/bdgeek
chown -R bdgeek:$GROUPNAME $HOME
cd $HOME && exec /usr/local/bin/gosu bdgeek $@
