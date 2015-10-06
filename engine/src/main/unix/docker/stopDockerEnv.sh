#!/bin/bash

stopDockerImage() {
    dockerImage=$1
    IS_UP=`docker ps|grep $dockerImage|wc -l`
    if [ "$IS_UP" -eq "0" ]; then
       echo "[$dockerImage] Docker Image already stopped"
    else
       echo "[$dockerImage] Stopping ..."
       docker stop $dockerImage 
    fi
}

stopDockerImage es
stopDockerImage cs
stopDockerImage mongo
stopDockerImage kf
stopDockerImage zk

#stopDockerImage skydock
#stopDockerImage skydns

