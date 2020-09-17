#!/bin/bash
source "$HOME/.sdkman/bin/sdkman-init.sh"
gpg --no-tty --batch --yes --import /root/gpg/sonatype_gpg.key
echo "$@"
exec "$@"
