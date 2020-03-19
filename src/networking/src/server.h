//lang:CwC
#pragma once
#include <stdio.h>  
#include <stdlib.h>  
#include <string.h>
#include <errno.h>  
#include <unistd.h>
#include <arpa/inet.h>
#include <sys/types.h>  
#include <sys/socket.h>  
#include <netinet/in.h>  
#include <sys/time.h>
#include "assert.h"
#include "../../object.h"
#include "../../string.h"
#include "../../serial/src/message.h"
#include "../../serial/src/serial.h"
#include <thread>
#include <functional>

#define BUFF_SIZE 1024

//class representing a Server object
class Server : public Object {
    public:
        String* serverIp;
        int serverPort_;
        int master_socket;
        int nodeCount_;
        Directory* nodeDir;
        int* sockets;
        struct sockaddr_in address;
        fd_set readfds, currentfds;
        unsigned char* buffer;
        bool running;
        std::thread* updateThread;

        Server(const char* ip, int serverPort, int nodeCount) {
            serverIp = new String(ip);
            serverPort_ = serverPort;
            nodeCount_ = nodeCount;
            nodeDir = new Directory(nodeCount);
            sockets = new int[nodeCount];
            memset(sockets, NULL, sizeof(sockets));
            running = false;
        }

        ~Server() {
            delete serverIp;
            delete buffer;
            delete sockets;
        }

        //launches a new thread that starts the server
        void serve() {
            updateThread = new std::thread(&Server::start, this);
        }

        //initializes the master_socket that all node traffic comes through
        void initialize() {
            buffer = new  unsigned char[BUFF_SIZE];
            int opt = 1;
            //create a master socket
            if( (master_socket = socket(AF_INET , SOCK_STREAM , 0)) == 0) {
                assert("Error creating master socket. Program terminated." && false);
            }
            if( setsockopt(master_socket, SOL_SOCKET, SO_REUSEADDR, (char*)&opt, sizeof(opt)) < 0) {
                assert("Error allowing multiple connections." && false);
            }
            address.sin_family = AF_INET;
            address.sin_addr.s_addr = inet_addr(serverIp->c_str());
            address.sin_port = htons(serverPort_);
            //bind the socket to serverPort
            if (bind(master_socket, (struct sockaddr *)&address, sizeof(address))<0) { 
                assert("Error binding master socket." && false);
            }
            if (listen(master_socket, nodeCount_) < 0) {
                assert("Failed to listen." && false);
            }
            p("Listening on port ").pln(serverPort_);
        }

        //Initializes and starts the listening process for node connections
        void start() {
            initialize();
            running = true;
            FD_ZERO(&currentfds);
            FD_SET(master_socket, &currentfds);

            while(running) {
                readfds = currentfds;
                //Waits for activity on the socket
                if (select(FD_SETSIZE, &readfds, NULL, NULL, NULL) < 0) {
                    assert("Error selecting." && false);
                }
                for (int i = 0; i < FD_SETSIZE; i++) {
                    if (FD_ISSET(i, &readfds)) {
                        if (i == master_socket) {
                            //accept incoming connection
                            int addrlen = sizeof(address);
                            for (int j = 0; j < nodeCount_; j++) {
                                if (sockets[j] == NULL) {
                                    if ((sockets[j] = accept(master_socket, (struct sockaddr*)&address, (socklen_t*)&addrlen)) < 0) {
                                        assert("Error accepting new socket." && false);
                                    }
                                    printf("New connection , socket fd is %d, port : %d, host: %s\n", sockets[j], ntohs(address.sin_port), inet_ntoa(address.sin_addr));
                                    FD_SET(sockets[j], &currentfds);
                                    break;
                                }
                            }
                        } else {
                            handleMessage(i, readIncoming(i));
                        }
                    }
                }
            }
        }

        //sends the given data to the socket at the given file descriptor
        void sendToNode(int fd, unsigned char* data) {
            if (send(fd, data, message_length(data), 0) != message_length(data)) {
                assert("Error sending data to node." && false);
            }
        }

        //reads incoming data into the buffer from the given file descriptor
        unsigned char* readIncoming(int fd) {
            //clear buffer
            memset(buffer, 0, BUFF_SIZE);
            int bytesRead = read(fd, buffer, BUFF_SIZE);
            if (bytesRead < 0) {
                assert("Error reading incoming data." && false);
            }
            if (bytesRead == 0) {
                handleDisconnect(fd);
            }
            unsigned char* newBuff = new unsigned char[BUFF_SIZE];
            memcpy(newBuff, buffer, BUFF_SIZE);
            //clear buffer
            memset(buffer, 0, BUFF_SIZE);
            return newBuff;
        }

        //primary message handler for incoming node messages
        void handleMessage(int fd, unsigned char* msg) {
            MsgKind kind = message_kind(msg);
            switch (kind) {
                case MsgKind::Register: {
                    handleRegistration(fd, msg);
                    break;
                }
                default: {
                    assert("Unrecognized message type" && false);
                }
            }
            nodeDir->print();
        }

        //handler for socket disconnections
        void handleDisconnect(int fd) {
            int addrlen = sizeof(address);
            getpeername(fd, (struct sockaddr*)&address ,(socklen_t*)&addrlen); 
            printf("Host disconnected, ip %s, port %d \n" ,inet_ntoa(address.sin_addr) , ntohs(address.sin_port));
            close(fd);
            FD_CLR(fd, &currentfds);
        }

        //message handler for registration Messages
        void handleRegistration(int fd, unsigned char* msg) {
            Register* rMsg = new Register();
            rMsg->deserialize(msg);
            if (nodeDir->addNode(rMsg->IP, rMsg->port)) {
                //notify client of success
                Ack* a = new Ack(MsgKind::Register);
                sendToNode(fd, a->serialize());
                delete a;
                //update all other clients with new list
                updateNodesWithDirectory();
            } else {
                //notify client of unsuccessful registration
                Nack* n = new Nack(MsgKind::Register);
                sendToNode(fd, n->serialize());
                delete n;
            }
            delete rMsg;
        }

        //sends new directory to all active node connections
        void updateNodesWithDirectory() {
            for (int i = 0; i < nodeCount_; i++) {
                if (sockets[i]) {
                    sendToNode(sockets[i], nodeDir->serialize());
                }
            }
        }

        // shutsdown the server
        void shutdown() {
            Kill* k = new Kill();
            for (int i = 0; i < nodeCount_; i++) {
                if (sockets[i]) {
                    sendToNode(sockets[i], k->serialize());
                }
            }
            delete k;
            pln("Shutting down.");
            exit(0);
        }
};