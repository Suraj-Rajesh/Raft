rm -rf Raft2/client/
rm -rf Raft2/server/
rm -rf Raft3/client/
rm -rf Raft3/server/
rm -rf Raft4/client/
rm -rf Raft4/server/
cp -r Raft/client/ Raft2/client
cp -r Raft/client/ Raft3/client
cp -r Raft/client/ Raft4/client
cp -r Raft/server/ Raft2/server
cp -r Raft/server/ Raft3/server
cp -r Raft/server/ Raft4/server
rm /home/sharath/Raft/persistence/*;
rm /home/sharath/Raft2/persistence/*;
rm /home/sharath/Raft3/persistence/*;
rm /home/sharath/Raft4/persistence/*;
cd Raft/server/
python raft_node.py &
cd ../../Raft2/server/
python raft_node.py &
cd ../../Raft3/server/
python raft_node.py &
