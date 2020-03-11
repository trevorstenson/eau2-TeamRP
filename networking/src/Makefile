build:
	g++ -g -pthread -std=c++11 master.cpp -o server
	g++ -g -pthread -std=c++11 node.cpp -o node

server:
	g++ -g -pthread -std=c++11 master.cpp -o server
	./server -ip 127.0.0.1

node:
	g++ -g -pthread -std=c++11 node.cpp -o node
	./node -ip 127.0.0.1 -port 9050

run:
	./server -ip 127.0.0.1 &
	./node -ip 127.0.0.1 -port 9050 &
	./node -ip 127.0.0.1 -port 9057

.PHONY: server node