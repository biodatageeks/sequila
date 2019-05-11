#!/bin/bash -x

BUILD_MODE=$1
IMAGE_TO_BUILD=$2
#only build images modified in the last 10h (10*3600s)
MAX_COMMIT_TS_DIFF=36000
version=`grep version build.sbt | cut -f2 -d'=' | sed 's/ //g' | sed 's/"//g'`
bump_version () {
  incl=0.01
  version="0.00"
  if [ "$(curl -L -s "https://registry.hub.docker.com/v2/repositories/${image}/tags" | jq -r ".detail")" == "Object not found" ]; then
    version="0.01"
  else
    version=`curl -L -s "https://registry.hub.docker.com/v2/repositories/${image}/tags"  | jq -r '.results[0].name '`
    version=`echo $version + $incl | bc| awk '{printf "%.2f\n", $0}'`
  fi
  echo $version
}



find Docker  -name "Dockerfile"  | sed 's/\/Dockerfile//' |grep "$IMAGE_TO_BUILD"| while read dir;
do

    echo $version
  image=`echo $dir| sed 's/^Docker/biodatageeks/'`
  #version=`if [ ! -e $dir/version ]; then bump_version $image; else tail -1 $dir/version; fi`
  #if [ -e $dir/version ]; then
    ver=$version
    if [[ $OSTYPE =~ darwin*. ]]; then
      sed -i '' "s/{{COMPONENT_VERSION}}/${ver}/g" $dir/Dockerfile ;
    else
      sed -i "s/{{COMPONENT_VERSION}}/${ver}/g" $dir/Dockerfile ;
    fi
  #fi
  echo "Building image ${image}..."
  #diffTs=`echo "$(date +%s) - $(git log -n 1 --pretty=format:%at ${dir})" | bc`
  #if [ $diffTs -lt $MAX_COMMIT_TS_DIFF ]; then
  if [ $image == "biodatageeks/bdg-sequila" ]; then
    cd $dir
     if [[ ${BUILD_MODE} != "local" ]]; then
         docker build --no-cache --build-arg BDG_VERSION=$version -t $image:$version .
     else
         docker build --no-cache --build-arg BDG_VERSION=$version -t $image:$version .
     fi
    docker build  -t $image:latest .
    if [[ ${BUILD_MODE} != "local" ]]; then
      docker push docker.io/$image:latest
      docker push docker.io/$image:$version
    fi
    ##revert COMPONENT_VERSION variable
    if [[ $OSTYPE =~ darwin*. ]]; then
        if [ -e version ]; then ver=$version; sed -i '' "s/${ver}/{{COMPONENT_VERSION}}/g" Dockerfile ; fi
    else
      if [ -e version ]; then ver=$version; sed -i  "s/${ver}/{{COMPONENT_VERSION}}/g" Dockerfile ; fi
    fi
    #keep only last 3 versions of an image locally (2+3 in tail part)
    docker images $image | tail -n +5 | sed 's/ \{1,\}/:/g' | cut -f1,2 -d':' | grep -v "<none>"| xargs -i docker rmi {}

    cd ../..
  fi

done
