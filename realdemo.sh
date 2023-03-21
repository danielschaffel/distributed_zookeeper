#Step 1
mvn dependency:build-classpath -Dmdep.outputFile=target/classpath.txt
path=`cat ./target/classpath.txt`
mvn -Dtest=Stage1Test test
mvn -Dtest=JavaRunnerTest test
mvn -Dtest=Stage4Test#testLeaderElected test
mvn -Dtest=Stage4Test#testServerElectedLeader test
pkill java
sleep 10s

arr=()
#Step 2
for i in {0..7}
do
	#java -cp ~/.m2/repository/gson-2.8.6.jar:./target/classes edu.yu.cs.fall2019.intro_to_distributed.stage4.Driver  $i &
	java -cp $path:./target/classes edu.yu.cs.fall2019.intro_to_distributed.stage4.Driver  $i &
	#mvn exec:java -Dexec.mainClass="edu.yu.cs.fall2019.intro_to_distributed.stage4.Driver" -Dexec.args="$i" &
	arr+=("$!")
done

#sleep 30s
sleep 1s
#Step 3
gatewaystatus="false"
while [[ "$gatewaystatus" == "false" ]] 
do
	gatewaystatus=$(curl -s localhost:8001/isthereleader)
	sleep 1s
done

curl -s localhost:8001/printstatus

#Step 4
for i in {0..8}
do
	curl -s http://localhost:3000/compileandrun --data-binary @./hello.java 
echo
done
echo

#Step 5
kill -9 "${arr[1]}"
echo "Killing server 1"

sleep 8s

curl -s localhost:8001/printstatus

#Step 6
for i in {0..8}
do
	curl -s -m 25  http://localhost:3000/compileandrun --data-binary @./hello.java &
done
sleep 1s
#Step 7
kill -9 ${arr[7]}

sleep 15s

gatewaystatus="false"
while [[ "$gatewaystatus" == "false" ]] 
do
	gatewaystatus=$(curl -s localhost:8001/isthereleader)
	sleep 1s
done

curl -s localhost:8001/printstatus

#Step 8
for i in {0..1}
do
	curl -s -m 10  http://localhost:3000/compileandrun --data-binary @./hello.java
done

sleep 10s
echo "server 2 Gossip history"
curl -s localhost:8021/gossipHistory
echo
echo "server 3 Gossip history"
curl -s localhost:8031/gossipHistory
echo
echo "server 4 Gossip history"
curl -s localhost:8041/gossipHistory
echo
echo "server 5 Gossip history"
curl -s localhost:8051/gossipHistory
echo
echo "server 6 Gossip history"
curl -s localhost:8061/gossipHistory
pkill java
sleep 10s
