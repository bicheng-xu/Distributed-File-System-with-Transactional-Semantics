if [ "$#" -ne 3 ]; then
    echo "3 parameters are required including ipaddress, port number and directory."
    exit
fi

java TCPServer $1 $2 $3
# echo $1 $2 $3

