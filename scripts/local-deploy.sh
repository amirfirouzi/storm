 #! /bin/zsh

host='green'
user='green'
password='123'
stormhome='/home/amir/Projects/storm/mystorm/storm'
stormstarter='examples/storm-starter'
#fgreen='/home/storm/storm'
remotestormhome='/home/green/vmware/storm-cluster-green-current/storm'
#remotestormhome=$fgreen
remotedeployscriptname='remote-deploy.sh'

version='2.0.0-SNAPSHOT'
stormpkgdir="$stormhome/storm-dist/binary/final-package/target"
stormpkgname="apache-storm-$version"
stormpkgnamezip="$stormpkgname.tar.gz"
stormpkg="$stormpkgdir/$stormpkgnamezip"

configpkgname="storm-config-files.tar.gz"

corename="storm-core-$version.jar"
corepkg="$stormpkgdir/$stormpkgname/lib/$corename"

topopkgdir="$stormpkgdir/$stormpkgname/$stormstarter"
topopkgname="storm-starter-topologies-$version"
topopkg="$topopkgdir/$topopkgname.jar"

options=$@
full_option=0
partial_option=0
compile_option=0
topo_option=0

check_failure() {
  #compile check
  if [ $? != 0 ]; then
    echo "@@@@@@-FAILED: $1. \nexitting..."1>&2
    exit 1
  else
    echo "@@@@@@-SUCCESS: $1."
  fi
}

log() {
    echo "@@@@@@ $1"
}

new_compilation(){
    # remove old extracted directory and untar new compiled storm tar and remove duplicate files from jars
    echo "==========New Compilation, New Extraction=========="
    log "remove old extracted package directory"
    rm -rf "$stormpkgdir/$stormpkgname"
    log "extracting new compiled storm tar.gz to local"
    cd $stormhome
    tar xzf $stormpkg -C $stormpkgdir/
    log "removing storm.yaml form core.jar"
    zip -d $corepkg storm.yaml

    if [ $topo_option = 1 ]; then
        log "removing storm.yaml and defaults.yaml from storm-starter.jar"
        zip -d $topopkg storm.yaml
        zip -d $topopkg defaults.yaml
    fi
    echo "==========New Compilation, New Extraction==========\n"
}

sshcommand() {
ssh "$1@$2" "$3"
}

echo "==========Script Options=========="
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
if [ $full_option = 1 ] && [ $partial_option = 1 ]; then
    echo "ERROR: Select either --full or --partial Option"
    exit 1
fi
echo "==========Script Options==========\n"

if [ $compile_option = 1 ] || [ $topo_option = 1 ]; then
    echo "==========Compiling Storm=========="
    mvn clean install -DskipTests=true
    check_failure "compiling"

    log "packaging storm"
    cd storm-dist/binary && mvn package && cd ../..
    check_failure "packaging binaries"
    echo "==========Compiling Storm==========\n"

    new_compilation
fi
echo "==========Deploy Storm to Remote=========="
#mkdir for remotestormhome directory
if (ssh "$user@$host" "[ -d $remotestormhome ]") then
    echo "$remotestormhome directory  exists, no need to mkdir"
else
    echo "$remotestormhome directory not found! creating..."
    sshcommand $user $host "mkdir $remotestormhome"
    echo "$remotestormhome directory created on $host"
fi
log "cleaning green storm home directory: $remotestormhome/*"
sshcommand $user $host "rm -rf $remotestormhome/*"
if [ $full_option = 1 ]; then
    log "full deploy: send storm package to remote"
    scp "$stormpkg" "$user@$host:$remotestormhome/"
elif [ $partial_option = 1 ]; then
    log "partial deploy: send storm-core jar to remote"
    scp $corepkg "$user@$host:$remotestormhome/"
fi
if [ $full_option = 0 ] && [ $topo_option = 1 ]; then
    log "topology option: send storm-starter topologies to remote"
    scp $topopkg "$user@$host:$remotestormhome/"
    sshcommand $user $host "bash -c 'chmod +x $remotestormhome/$topopkgname.jar;'"
fi

currentdir=$PWD
cd scripts
log "compress config files into zip file"
tar -zcf $configpkgname config/
log "send config files to remote"
scp "$configpkgname" "$user@$host:$remotestormhome/"
log "extracting log files in remote"
sshcommand $user $host "bash -c 'tar xzf $remotestormhome/$configpkgname -C $remotestormhome/;'"
log "send $remotedeployscriptname to Remote Server"
scp "$remotedeployscriptname" "$user@$host:$remotestormhome/"
cd "../"
echo "==========Deploy Storm to Remote=========="
echo "================ ****** =================="
