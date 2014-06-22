#!/bin/bash

NAME="evangelist"                    # Name of the application
DIRECTORY="/home/deploy/evangelist"  # Location of application
USER=deploy                          # User to run as
 
echo "Starting $NAME as `whoami`"
 
cd $DIRECTORY
exec su $USER -c '. ~/.bash_profile && . env.sh && go run server.go'
