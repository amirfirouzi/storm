 #! /bin/zsh

options=$@
user='green'
host='green'
remotedeployscriptname='remote-deploy.sh'
remotestormhome='/home/green/vmware/storm-cluster-green-current/storm'
remotedeployscriptfile="$remotestormhome/$remotedeployscriptname"

echo "***************************************"
echo "@@@@@@@@@@ LOCAL DEPLOYMENT @@@@@@@@@@@"
echo "***************************************\n"
local-deploy.sh $options
echo "@@@@@@@@@@ **************** @@@@@@@@@@@"
echo "@@@@@@@@@@ LOCAL DEPLOYMENT @@@@@@@@@@@"
echo "@@@@@@@@@@ **************** @@@@@@@@@@@\n"

echo "***************************************"
echo "@@@@@@@@@@ SERVER DEPLOYMENT @@@@@@@@@@"
echo "***************************************"
ssh -t "$user@$host" "bash -c 'chmod +x $remotedeployscriptfile; $remotedeployscriptfile $options;'"
echo "***************************************"
echo "@@@@@@@@@@ SERVER DEPLOYMENT @@@@@@@@@@"
echo "***************************************\n"