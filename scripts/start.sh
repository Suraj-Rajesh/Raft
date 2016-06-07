python ../server/raft_node.py &
sleep 2
tail -f ../log/raft_node.log
