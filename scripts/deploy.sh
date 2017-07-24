 #! /bin/zsh

check_failure() {
  #compile check
  if [ $? != 0 ]; then
    echo "\n@@@@@@-FAILED: $1. \nexitting...\n"1>&2
    exit 1
  else
    echo "\n@@@@@@-SUCCESS: $1.\n"
  fi
}

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

sudosshcommand() {
ssh -t "$1@$2" "sudo $3"
}


user='storm'
password=123
masternode='stormmaster'
nodes='stormmaster stormslave1 stormslave2 stormslave3 stormslave4 stormslave5 stormslave6'
stormhome='/home/amir/Projects/storm/mystorm/storm'
version='2.0.0-SNAPSHOT'

stormpkgdir="$stormhome/storm-dist/binary/final-package/target"
stormpkgname="apache-storm-$version"
stormpkgnamezip="$stormpkgname.tar.gz"
stormpkg="$stormpkgdir/$stormpkgnamezip"

corename="storm-core-$version.jar"
corepkg="$stormpkgdir/$stormpkgname/lib/$corename"

topopkgdir="$stormpkgdir/$stormpkgname/examples/storm-starter"
topopkgname="storm-starter-topologies-$version"
topopkg="$topopkgdir/$topopkgname.jar"

full_option=0
compile_option=0
topo_option=0
#get options
for i in "$@"
do
    case $i in
        -c|--compile)
        compile_option=1
        echo "compile option detected. compile first"
        shift # past argument
        ;;
        -f|--full)
        full_option=1
        echo "full option detected. full deploy(copy all files)"
        shift # past argument
        ;;
        -t|--topology)
        topo_option=1
        echo "topology option detected. copy new topologies"
        shift # past argument
        ;;
        *)
                # unknown option
        ;;
    esac
done

if [ $compile_option != 0 ]; then
    mvn clean install -DskipTests=true
    check_failure "compiling"

    cd storm-dist/binary && mvn package && cd ../..
    check_failure "packaging binaries"
fi

backupdir='backup'
stormdir='storm-current'

#remove old extracted package directory
rm -rf "$stormpkgdir/$stormpkgname"
#extract new packaged storm to local
echo "extracting new storm core zip to local"
cd $stormhome
tar xzf $stormpkg -C $stormpkgdir/
echo "removing storm.yaml and db.ini form core.jar"
zip -d $corepkg storm.yaml
zip -d $corepkg db.ini

if [ $topo_option != 0 ]; then
    echo "removing storm.yaml and db.ini from storm-starter.jar"
    zip -d $topopkg storm.yaml
    zip -d $topopkg db.ini
    echo "copying topology package to ~/packages of master node: " $masternode
    scp "$topopkg" "$user@$masternode:~/packages/"
fi

for i in $nodes
do
    check_ssh $i
    if [ $connected != 0 ]; then
        #mkdir for backup directory
        if (ssh "$user@$i" "[ -d $backupdir ]") then
            echo "$backupdir directory  exists, no need to mkdir"
        else
	        echo "$backupdir directory not found! creating..."
	        sshcommand $user $i "mkdir $backupdir"
	        echo "$backupdir directory created on $i"
        fi
        #make tar file form current storm directory and remove existing-name:storm-timestamp.tar.gz
        #make copy of slave storm.yaml file in backup/
        echo "creating backup on $i"
        echo "copying storm.yaml & db.ini files to backup/"
        scp "$stormhome/scripts/config/$i/storm.yaml" "$user@$i:$backupdir/"
        scp "$stormhome/scripts/config/$i/db.ini" "$user@$i:$backupdir/"

        echo "stopping supervisord daemons"
        ssh -t "$user@$i" "echo $password | /usr/bin/sudo -S bash -c 'supervisorctl stop all;'"
        #if full: remove storm-current directory content & copy new package & config files to storm-current
        #else   : remove only storm-core & copy new storm-core and config files
        if [ $full_option != 0 ]; then
            echo "\n@@@Full Deployment needed. replacing whole installation"
            echo "removing storm-current content except logs/"
            sshcommand $user $i "find $stormdir -mindepth 1 -name logs -prune -o -exec rm -rf {} \;"
            echo "copying new tar package"
            cd "$stormpkgdir"
            rm $stormpkg
            tar -zcf "$stormpkgnamezip" "$stormpkgname"
            scp $stormpkg "$user@$i:$stormdir/"
            echo "extracting storm package & copying config files to storm-current & restore log files"
            ssh -t "$user@$i" "echo $password | /usr/bin/sudo -S bash -c 'tar xzf $stormdir/$stormpkgnamezip --strip 1 -C $stormdir/; rm $stormdir/$stormpkgnamezip;cp $backupdir/storm.yaml $stormdir/conf/; cp $backupdir/db.ini $stormdir/;chown -R $user:$user $stormdir/*; chmod -R 775 $stormdir/*; echo starting supervisord daemons;supervisorctl start all;'"
            cd $stormhome
        else
            echo "\n@@@Partial Deployment needed. replacing just the storm-core.jar"
            echo "removing $stormdir/lib/$corename"
            sshcommand $user $i "rm -rf $stormdir/lib/$corename"

            echo "copying new storm-core.jar file to storm-current"
            scp $corepkg "$user@$i:$stormdir/lib/"

            ssh -t "$user@$i" "echo $password | /usr/bin/sudo -S bash -c 'cp $backupdir/storm.yaml $stormdir/conf/; cp $backupdir/db.ini $stormdir/;echo starting supervisord daemons;supervisorctl start all;'"

        fi
    fi
done
