#!/bin/bash

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
version=`grep 'version :=' ../build.sbt | sed  's|version := \"||g' | sed 's|\"||g'`
echo Version is $version

# substitute
if [[ $OSTYPE =~ darwin*. ]]; then
	find source_v -type f -name "*.rst" -exec sed -i '' "s|\|version\||$version|g" {} +
	find source_v -type f -name "*.rst" -exec sed -i '' "s|\|project_name\||$project_name|g" {} +
else
	find source_v -type f -name "*.rst" -exec sed -i "s|\|version\||$version|g" {} +
	find source_v -type f -name "*.rst" -exec sed -i "s|\|project_name\||$project_name|g" {} +
fi
# create html docs
make html 


