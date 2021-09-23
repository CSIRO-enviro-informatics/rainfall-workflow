DOCKER=`which docker`
DOCKER_IMAGE=osgeo/gdal:ubuntu-full-latest
MOUNTS="-v/datasets/work/lw-soildatarepo/work/RainfallWorkflow:/datasets/work/lw-soildatarepo/work/RainfallWorkflow"
CONTAINER_NAME=access_g_cogs_run_1
INDIR=/datasets/work/lw-soildatarepo/work/RainfallWorkflow/ACCESS-G-APS2-3/
OUTDIR=/datasets/work/lw-soildatarepo/work/RainfallWorkflow/ACCESS-G-APS3-COG/
mkdir -p "$OUTDIR"
MY_ID=`id -u`
MY_GID=`id -g`
RUNNING=$($DOCKER run --name $CONTAINER_NAME --rm -i -t -u "$MY_ID":"$MY_GID" --detach $MOUNTS $DOCKER_IMAGE tail -f /dev/null)
ACCESS_G_COGS_SH=./accessg_to_cogs.sh

find "$INDIR" -type f -iname "*12.nc" -not -iname "*.cog.tif" -exec $ACCESS_G_COGS_SH {} "$INDIR" "$OUTDIR" \;

$DOCKER stop $CONTAINER_NAME && $DOCKER rm $CONTAINER_NAME