 #! /bin/bash

echo "================ ****** ==================="
echo "==========Remote Server Deploying=========="

options=$@
full_option=0
partial_option=0
compile_option=0
topo_option=0

user='storm'
password='123'
masternode='stormmaster'
nodes='stormmaster stormslave1 stormslave2 stormslave3 stormslave4 stormslave5 stormslave6'
stormdir="/home/$user/storm-current"
fgreen='/home/storm/storm'
#remotestormhome='/home/green/vmware/storm-cluster-green-current/storm'
remotestormhome=$fgreen
nodehomedir="/home/$user"
version='2.0.0-SNAPSHOT'

stormpkgname="apache-storm-$version"
stormpkgnamezip="$stormpkgname.tar.gz"
stormpkg="$remotestormhome/$stormpkgnamezip"

configpkgname="storm-config-files.tar.gz"
configpkg="$remotestormhome/$configpkgname"
configdir="$remotestormhome/config"

corename="storm-core-$version.jar"
corepkg="$remotestormhome/$corename"

topopkgname="storm-starter-topologies-$version"
topopkg="$remotestormhome/$topopkgname.jar"

check_ssh() {
    nc -z $1 22 > /dev/null
    if [ $? != 0 ]; then
        echo "--------------------------------------"
        echo "@@@@@@-$1 is not available"
        echo "--------------------------------------"
        connected=0
    else
        echo "--------------------------------------"
        echo "@@@@@@-$1 is available"
        echo "--------------------------------------"
        connected=1
    fi
}

sshcommand() {
ssh "$1@$2" "$3"
}

log() {
    echo "@@@@@@ $1"
}

for i in "$@"
do
    case $i in
        -c|--compile)
        compile_option=1
        log "compile option detected. compile first"
        shift # past argument
        ;;
        -f|--full)
        full_option=1
        log "full option detected. full deploy will be done(copy all files)"
        shift # past argument
        ;;
        -p|--partial)
        partial_option=1
        log "partial option detected. partial deploy will be done(copy only storm-core.jar file)"
        shift # past argument
        ;;
        -t|--topology)
        topo_option=1
        log "topology option detected. copy new topologies"
        shift # past argument
        ;;
        *)
                # unknown option
        ;;
    esac
done

send_config_files(){
    scp "$configdir/$i/storm.yaml" "$user@$i:$nodehomedir/packages/"
    scp "$configdir/$i/db.ini" "$user@$i:$nodehomedir/packages/"
}

for i in $nodes
do
    check_ssh $i
    if [ $connected = 1 ]; then
        if [ $full_option = 1 ]; then
            log "Full Deployment needed. replacing whole installation"

            log "send config files to $i"
            send_config_files

            log "stopping supervisord daemons"
            ssh -t "$user@$i" "echo $password | /usr/bin/sudo -S bash -c 'supervisorctl stop all;'"

            log "removing storm-current:$stormdir/ content except logs/"
            sshcommand $user $i "find $stormdir/ -mindepth 1 -name logs -prune -o -exec rm -rf {} \;"
            log "copying new tar package"
            scp $stormpkg "$user@$i:$nodehomedir/packages"
            log "extracting storm package & copying config files to storm-current"
            ssh -t "$user@$i" "echo $password | /usr/bin/sudo -S bash -c 'tar xzf $nodehomedir/packages/$stormpkgnamezip --strip 1 -C $stormdir/; rm $nodehomedir/packages/$stormpkgnamezip; cp $nodehomedir/packages/storm.yaml $stormdir/conf/; cp $nodehomedir/packages/db.ini $stormdir/;chown -R $user:$user $stormdir/*; chmod -R 775 $stormdir/*; echo starting supervisord daemons;supervisorctl start all;'"
            cd $stormhome
#        else
#            echo "\n@@@Partial Deployment needed. replacing just the storm-core.jar"
#            echo "removing $stormdir/lib/$corename"
#            sshcommand $user $i "rm -rf $stormdir/lib/$corename"
#
#            echo "copying new storm-core.jar file to storm-current"
#            scp $corepkg "$user@$i:$stormdir/lib/"
#
#            ssh -t "$user@$i" "echo $password | /usr/bin/sudo -S bash -c 'cp $backupdir/storm.yaml $stormdir/conf/; cp $backupdir/db.ini $stormdir/;echo starting supervisord daemons;supervisorctl start all;'"

        fi
    fi
done