 #! /bin/zsh

user='storm'
nodes='stormmaster stormslave1 stormslave2 stormslave3 stormslave4 stormslave5 stormslave6'
stormhome='/home/amir/Projects/storm/mystorm/storm'
pkgname='apache-storm-2.0.0-SNAPSHOT'
pkgnameext="$pkgname.tar.gz"
stormpkg="storm-dist/binary/final-package/target/$pkgnameext"
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
        echo "\n@@@@@@-$1 is not available"
        connected=0
    else
        echo "\n@@@@@@-$1 is available"
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

#cd storm-dist/binary && mvn package && cd ../..
#check_failure "packaging binaries"

backupdir='backup'
stormdir='storm-current'
for i in $nodes
do
    check_ssh $i
    if [ $connected != 0 ]; then
        #mkdir for backup directory
        if (ssh "$user@$i" "[ -d $backupdir ]") then
            echo "$backupdir directory  exists! no need to mkdir"
        else
	        echo "$backupdir directory not found! creating..."
	        sshcommand $user $i "mkdir $backupdir"
	        echo "$backupdir directory created on $i"
        fi
	    

        #make tar file form current storm directory and remove existing-name:storm-timestamp.tar.gz
        #make copy of slave storm.yaml file in backup/
        echo "creating backup on $i"
        echo "removing latest backup file"
        sshcommand $user $i "rm  $backupdir/storm-backup.tar.gz"
        #TODO: if not exists storm.yaml in backup dir
        echo "copying storm.yaml file"
        sshcommand $user $i "cp $stormdir/conf/storm.yaml $backupdir/"
#        echo "making tar file"
#        sshcommand $user $i "tar -czf $backupdir/storm-backup.tar.gz $stormdir/*"

        #remove storm-current directory content
        echo "removing storm-current content"
        sshcommand $user $i "rm -rf $stormdir/*"

        #copy new package to node at storm-current location
        echo "copying new tar package"
        cd $stormhome
        if [ $full != 0 ]; then
            scp $stormpkg "$user@$i:$stormdir/"
            echo "extracting storm package"
            ssh -t "$user@$i" "/usr/bin/sudo bash -c 'tar xzf $stormdir/$pkgnameext --strip 1 -C $stormdir/; rm $stormdir/$pkgnameext;chown -R $user:$user $stormdir/*; chmod -R 775 $stormdir/*;'"
            #TODO: copy backup/storm.yaml to conf dir
            #TODO: copy db.ini to conf dir
#        else
        fi
        
        #restart services: nimbus/supervisor and supervisor daemnons
    fi
done
