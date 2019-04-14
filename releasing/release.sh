#!/usr/bin/env bash

#publish
sbt "set test in publish := {}" clean publish

#assembly
sbt "set test in assembly := {}" clean assembly
