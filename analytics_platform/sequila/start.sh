#!/bin/bash -x
while [ $# -gt 0 ]; do
  case "$1" in
    --master=*)
      D_SEQUILA_MASTER="${1#*=}"
      ;;
    --driver-memory=*)
      D_SEQUILA_DRIVER_MEM="${1#*=}"
      ;;
    --data-dir=*)
      D_DATA="${1#*=}"
      ;;
    --sequila-version=*)
      D_SEQUILA_VERSION="${1#*=}"
      ;;
    --superset-version=*)
      D_SUPERSET_VERSION="${1#*=}"
      ;;
    *)
      printf "***************************\n"
      printf "* Error: Invalid argument.*\n"
      printf "***************************\n"
      printf "e.g. ./start.sh --master=local[2] --driver-memory=2g --data-dir=/data/input/bams  --sequila-version=0.5.2 --superset-version=0.28.1"
      exit 1
  esac
  shift
done

D_UID=${UID} D_GID=$(id -g) D_SUPERSET_VERSION=${D_SUPERSET_VERSION} D_SEQUILA_VERSION=${D_SEQUILA_VERSION} D_METASTORE_VERSION=1.2.x D_SEQUILA_MASTER=${D_SEQUILA_MASTER} D_SEQUILA_DRIVER_MEM=${D_SEQUILA_DRIVER_MEM} D_DATA=${D_DATA} docker-compose up -d
sleep 5
docker exec -it sequila_bdg-superset_1  superset-init
