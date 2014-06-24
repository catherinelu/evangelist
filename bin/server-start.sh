#!/bin/bash

NAME="evangelist"                    # Name of the application
DIRECTORY="/home/deploy/evangelist"  # Location of application
 
echo "Starting $NAME as `whoami`"
 
cd $DIRECTORY
. ~/.bash_profile
source env.sh
go run server.go scoryst us-west-2
