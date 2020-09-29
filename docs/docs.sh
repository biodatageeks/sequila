#!/bin/bash -x

#all|latexpdf|html
BUILD_MODE=$1

# source directory which we are updating (with sphinx variables)
# source_v is generated with variables substitution 
# source_v is used for sphinx-build



# remove generated directory
rm -rf source_v

# copy new source files
cp -r source source_v

#extract name
project_name=`grep 'name :=' ../build.sbt | sed  's|name := \"\"\"||g' | sed 's|\"\"\"||g'`
echo Project name is $project_name

# extract version
version=$VERSION
echo Version is $version

# substitute
if [[ $OSTYPE =~ darwin*. ]]; then
	find source_v -type f -name "*.rst" -exec sed -i '' "s|\|version\||$version|g" {} +
	find source_v -type f -name "*.rst" -exec sed -i '' "s|\|project_name\||$project_name|g" {} +
	find source_v -type f -name "conf.py" -exec sed -i '' "s|\|version\||$version|g" {} +

else
	find source_v -type f -name "*.rst" -exec sed -i "s|\|version\||$version|g" {} +
	find source_v -type f -name "*.rst" -exec sed -i "s|\|project_name\||$project_name|g" {} +
	find source_v -type f -name "conf.py" -exec sed -i "s|\|version\||$version|g" {} +
fi
# create html docs
 if [[ ${BUILD_MODE} == "pdf" ]]; then
    make latexpdf
 elif   [[ ${BUILD_MODE} == "html" ]]; then
    make html
 else
    make latexpdf
    make html
 fi



