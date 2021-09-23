#!/bin/sh

MY_UID=`id -u`
if [ -z "$WORKFLOW_UID" ] ; then
  USE_UID="0"
else
  USE_UID="$WORKFLOW_UID"
fi

if [ -z "$WORKFLOW_GID" ] ; then
  USE_GID="0"
else
  USE_GID="$WORKFLOW_GID"
fi
echo "Docker Container Entrypoint reached, UID=$MY_UID, USE_UID=$USE_UID"
if [ "0" = "$USE_UID" -a "0" = "$USE_GID" ] ; then
  echo "Running Rainfall Workflow as container's ROOT user. This won't go well."
else
  echo "Using UID=$USE_UID GID=$USE_GID"
fi

if [ "0" = "$MY_UID" ] ; then
  echo "Attempting to change from workflow root user to the specified workflow user."
  chown -R "$USE_UID:$USE_GID" /workflow
  chmod -R 777 /workflow
  exec su-exec "$USE_UID:$USE_GID" "$@"
else
  echo "You are trying to run the Workflow container using docker's USER command. Cannot use Root to change ownership of the project files. You may experience errors."
  exec "$@"
fi

