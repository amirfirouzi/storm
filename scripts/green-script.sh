 #! /bin/zsh

user='green'
password='green845'
stormhome='/home/amir/Projects/storm/mystorm/storm'
stormstarter='examples/storm-starter'
greenstormhome='/home/green/vmware/storm-cluster-green-current/storm'
version='2.0.0-SNAPSHOT'

stormpkgdir="$stormhome/storm-dist/binary/final-package/target"
stormpkgname="apache-storm-$version"
stormpkgnamezip="$stormpkgname.tar.gz"
stormpkg="$stormpkgdir/$stormpkgnamezip"

corename="storm-core-$version.jar"
corepkg="$stormpkgdir/$stormpkgname/lib/$corename"

topopkgdir="$stormhome/$stormstarter/target"
topopkgname1="storm-starter-$version"
topopkgname2="storm-starter-topologies-$version"
topopkg1="$topopkgdir/$topopkgname1.jar"
topopkg2="$topopkgdir/$topopkgname2.jar"

check_failure() {
  #compile check
  if [ $? != 0 ]; then
    echo "\n@@@@@@-FAILED: $1. \nexitting...\n"1>&2
    exit 1
  else
    echo "\n@@@@@@-SUCCESS: $1.\n"
  fi
}

log() {
    echo "\n@@@@@@ $1 @@@@@@\n"
}

full_option=0
compile_option=0
topo_option=0
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
        log "full option detected. full deploy(copy all files)"
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

if [ $compile_option != 0 ]; then
#    log "compiling storm"
#    mvn clean install -DskipTests=true
#    check_failure "compiling"
#
#    log "packaging storm"
#    cd storm-dist/binary && mvn package && cd ../..
#    check_failure "packaging binaries"

    log "send storm package to green"
    scp "$stormpkg" "$user@$user:$greenstormhome/"
else
   if [ $topo_option != 0 ]; then
#        currentdir=$PWD
#        cd $stormhome/$stormstarter
#        log "compiling topologies"
#        mvn clean install -DskipTests=true
#        check_failure "compiling topologies"
#
#        log "packaging topologies"
#        mvn package
#        check_failure "packaging topologies"
#        cd $currentdir
        #TODO: maybe it's better to send apache-storm compiled version not examples
        log "send topologies package to green"
    scp "$topopkg1" "$user@$user:$greenstormhome/"
   fi
fi

log "send config files to green"
currentdir=$PWD
cd scripts
tar -zcf "storm-config-files.tar.gz" "config/"
cd "../"