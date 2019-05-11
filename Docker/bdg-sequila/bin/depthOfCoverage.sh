#!/bin/bash

appParams=`echo $@ | sed 's/-- /|/g' | cut -f2 -d'|'| sed 's/-F SAF//g'` ###temp remove format option
sparkParams=`echo $@ | sed 's/-- /|/g' | cut -f1 -d'|'`
#iappParams=`echo $@ | sed 's/ -- /|/g' | cut -f2 -d'|'`
readsFile=`echo ${@:$#}`

substr="master"
if case ${sparkParams} in *"${substr}"*) true;; *) false;; esac; then
    echo "Master specified"
 else
    echo "Master not specified, adding --master=local[*]"
    master=" --master local[*] "
    sparkParams=$sparkParams$master
 fi


outfile=`echo $appParams | sed -n "s/^.*-o \([^ ]*\) .*$/\1/p"`

echo "Checking output directory " $outfile

if [ -e "$outfile" ]
then
    echo "Output directory already exists, please remove"
    exit 1;
fi


echo '

   _____      ____        _ __                ____        ______
  / ___/___  / __ \__  __(_) /   ____ _      / __ \____  / ____/
  \__ \/ _ \/ / / / / / / / /   / __ `/_____/ / / / __ \/ /
 ___/ /  __/ /_/ / /_/ / / /___/ /_/ /_____/ /_/ / /_/ / /___
/____/\___/\___\_\__,_/_/_____/\__,_/     /_____/\____/\____/

                                                          '
echo $BDG_VERSION
echo -e "\n"
echo "Running with the following arguments: $appParams"
echo "Arguments passed to Apache Spark: $sparkParams"
echo -e "\n"
spark-submit ${sparkParams} --class org.biodatageeks.apps.DepthOfCoverage /tmp/bdg-toolset/bdg-sequila-assembly-${BDG_VERSION}.jar  $appParams
