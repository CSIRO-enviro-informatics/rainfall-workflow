#!/bin/bash
DOCKER=`which docker`
DOCKER_IMAGE=osgeo/gdal:ubuntu-full-latest
#MOUNTS="-v /q3774/landscapes-aet:/q3774/landscapes-aet"
CONTAINER_NAME=access_g_cogs_run_1
#RUNNING=$($DOCKER run --name $CONTAINER_NAME --rm -i -t --detach $MOUNTS $DOCKER_IMAGE tail -f /dev/null)
NC_VARIABLE="accum_prcp"
INFILE="$1"
INDIR="$2"
INFILENAME="$(basename -- $INFILE)"
INPATH="${INFILE%$INFILENAME}"
INPATHREL="${INPATH#$INDIR}"
OUTDIR="$3"
OUTFILENAME="$OUTDIR""$INPATHREL""${INFILENAME%.*}.cog.tif"
INNCSPEC="NETCDF:\"""$INFILE""\":""$NC_VARIABLE"
if [ -f "$OUTFILENAME" ] ; then
  echo "$OUTFILENAME exists"
  exit 0
fi
mkdir -p "$OUTDIR""$INPATHREL"
NODATA="-a_nodata 9.9999996e+35"
COMPRESS="-co COMPRESS=DEFLATE"
EXEC="$DOCKER exec -i -t $CONTAINER_NAME gdal_translate -stats -sds -of COG $NODATA $COMPRESS -co NUM_THREADS=ALL_CPUS --config GDAL_CACHEMAX 512"
exec $EXEC $INNCSPEC $OUTFILENAME
