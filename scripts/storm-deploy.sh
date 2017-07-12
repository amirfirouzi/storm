 #! /bin/zsh

user='storm'
nodes='stormmaster stormslave1 stormslave2 stormslave3 stormslave4 stormslave5 stormslave6'
 
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

#mvn clean install -DskipTests=true
#check_failure "compiling"

#cd storm-dist/binary && mvn package
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
        echo "removing last backup file"
        sshcommand $user $i "rm  $backupdir/storm-backup.tar.gz"
        echo "copying storm.yaml file"
        sshcommand $user $i "cp $stormdir/conf/storm.yaml $backupdir/"
        echo "making tar file"
        sshcommand $user $i "tar -czf $backupdir/storm-backup.tar.gz storm-current/*"
        
        #copy new package to node at storm-current location
        
        #unzip package
        
        #copy yaml files to specified location: storm-current/conf
        #stormmaster/storm.yaml for master nodes & stormslaves/storm.yaml for slave nodes
        
        #restart services: nimbus/supervisor and supervisor daemnons
    fi
done
