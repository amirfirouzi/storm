 #! /bin/zsh

user='storm'
password=123
nodes='stormmaster stormslave1 stormslave2 stormslave3 stormslave4 stormslave5 stormslave6'
stormhome='/home/amir/Projects/storm/mystorm/storm'
version='2.0.0-SNAPSHOT'
stormpkgdir="$stormhome/storm-dist/binary/final-package/target"
pkgname="apache-storm-$version"
pkgnamezip="$pkgname.tar.gz"
corename="storm-core-$version.jar"
corepkg="$stormpkgdir/$pkgname/lib/$corename"
stormpkg="$stormpkgdir/$pkgnamezip"
full=1
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
        echo "\n--------------------------------------"
        echo "@@@@@@-$1 is not available"
        echo "--------------------------------------"
        connected=0
    else
        echo "\n--------------------------------------"
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
#mvn clean install -DskipTests=true
#check_failure "compiling"
#
#cd storm-dist/binary && mvn package && cd ../..
#check_failure "packaging binaries"

backupdir='backup'
stormdir='storm-current'

#remove old extracted package directory
rm -rf "$stormpkgdir/$pkgname"
#extract new packaged storm to local
echo "extracting new storm core zip to local"
cd $stormhome
tar xzf $stormpkg -C $stormpkgdir/
echo "removing storm.yaml and db.ini form core.jar"
zip -d $corepkg storm.yaml
zip -d $corepkg db.ini

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
        if [ $full != 0 ]; then
            echo "\n@@@Full Deployment needed. replacing whole installation"
            echo "removing storm-current content except logs/"
            sshcommand $user $i "find $stormdir -mindepth 1 -name logs -prune -o -exec rm -rf {} \;"
            echo "copying new tar package"
            cd "$stormpkgdir"
            rm $stormpkg
            tar -zcf "$pkgnamezip" "$pkgname"
            scp $stormpkg "$user@$i:$stormdir/"
            echo "extracting storm package & copying config files to storm-current & restore log files"
            ssh -t "$user@$i" "echo $password | /usr/bin/sudo -S bash -c 'tar xzf $stormdir/$pkgnamezip --strip 1 -C $stormdir/; rm $stormdir/$pkgnamezip;chown -R $user:$user $stormdir/*; chmod -R 775 $stormdir/*; cp $backupdir/storm.yaml $stormdir/conf/; cp $backupdir/db.ini $stormdir/conf/;echo starting supervisord daemons;supervisorctl start all;'"
            cd $stormhome
        else
            echo "\n@@@Partial Deployment needed. replacing just the storm-core.jar"
            echo "removing $stormdir/lib/$corename"
            sshcommand $user $i "rm -rf $stormdir/lib/$corename"

            echo "copying new storm-core.jar file to storm-current"
            scp $corepkg "$user@$i:$stormdir/lib/"

            ssh -t "$user@$i" "echo $password | /usr/bin/sudo -S bash -c 'cp $backupdir/storm.yaml $stormdir/conf/; cp $backupdir/db.ini $stormdir/conf/;echo starting supervisord daemons;supervisorctl start all;'"

        fi
#        echo "starting supervisord daemons"
#        ssh -t "$user@$i" "echo $password | /usr/bin/sudo -S bash -c 'supervisorctl start all;'"
        #restart services: nimbus/supervisor and supervisor daemnons
    fi
done
