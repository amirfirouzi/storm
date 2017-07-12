DIR="$1"
user="storm"
host="stormmaster"
if (ssh "$user@$host" "[ -d $DIR ]")
then
    echo "$DIR directory  exists!"
else
    echo "$DIR directory not found!"
fi
