#!/usr/bin/env bash
export APP_SLACK_ICON_EMOJI=:hammer_and_pick:
export APP_SLACK_USERNAME=biodatageek-tester

seq 5 5 20 | xargs -I {} sh -c "slack '#project-genomicranges' 'Running a performance test using {} Spark Executors with 4 cores each...' ; bin/run_perf_test.sh --script=performance/test_min_overlap_cluster.scala --iterations=2 --master=yarn --spark-home=/data/local/opt/spark-2.2.1-bin-hadoop2.7 --executor-memory=8g --num-executors={} --executor-cores=4  --driver-memory=8g"
slack '#project-genomicranges' 'All tests completed!'