rm ../config/config.ini
cp ../config/config0.ini ../config/config.ini
rm ../persistence/*.p
python ../server/raft_node.py &
sleep 2
tail -f ../log/raft_node.log
