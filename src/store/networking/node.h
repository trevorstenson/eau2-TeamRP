//lang:CwC
#pragma once

#include <atomic>
#include <stdio.h>  
#include <stdlib.h>  
#include <errno.h>  
#include <unistd.h>
#include <arpa/inet.h>
#include <sys/types.h>  
#include <sys/socket.h>  
#include <netinet/in.h>  
#include <sys/time.h>
#include <sys/ioctl.h>
#include "assert.h"
#include "../../object.h"
#include "../../string.h"
#include "../../serial/message.h"
#include "../../serial/serial.h"
#include <thread>
#include <functional>
#include <mutex>

#define TEMP_CLIENTS_MAX 30
#define BUFF_SIZE 1024

//class representing a Node within our network
class Node : public Object {
    public:
        // Node's IP and port
        String* ip_;
        size_t port_;
        // Server's IP and port
        String* serverIp_;
        size_t serverPort_;
        // FDs for sending to neighbor sockets
        int* neighborSockets;
        Directory* nodeDir;
        int serverSocket_;
        int clientSocket_;
        struct sockaddr_in clientaddr;
        unsigned char* serverBuffer;
        unsigned char* neighborBuffer;
        std::thread* listenToServerThread;
        std::thread* listenToNeighborsThread;
        fd_set neighborReadFds, neighborCurrentFds;
        atomic<bool> running;

        Node(const char* ip, int port, const char* serverIp, int serverPort) {
            ip_ = new String(ip);
            serverIp_ = new String(serverIp);
            port_ = port;
            serverPort_ = serverPort;
            serverBuffer = new unsigned char[BUFF_SIZE];
            neighborBuffer = new unsigned char[BUFF_SIZE];
            memset(serverBuffer, 0, BUFF_SIZE);
            memset(neighborBuffer, 0, BUFF_SIZE);
            neighborSockets = new int[TEMP_CLIENTS_MAX - 1];
            memset(neighborSockets, NULL, sizeof(neighborSockets));
            running = false;
            listenToServerThread = nullptr;
            listenToNeighborsThread = nullptr;
        }

        Node(const char* ip, const char* serverIp, int serverPort) : Node(ip, serverPort, serverIp, serverPort) { }

        ~Node() {
            delete ip_;
            delete serverIp_;
            delete[] neighborSockets;
            delete nodeDir;
            delete[] serverBuffer;
            delete[] neighborBuffer;
            delete listenToNeighborsThread;
            delete listenToServerThread;
        }
        
        //sends the given data to the server socket
        void sendToServer(unsigned char* msg) {
            if (send(serverSocket_, msg, message_length(msg), 0) < 0) {
                assert("Error sending data to server." && false);
            }
        }

        //registers this Node with the server
        void registerWithServer() {
            serverSocket_ = socket(AF_INET, SOCK_STREAM, 0);
            if (serverSocket_ < 0) {
                assert("Error creating socket." && false);
            }
            struct sockaddr_in servaddr;
            servaddr.sin_family = AF_INET;
            servaddr.sin_addr.s_addr = inet_addr(serverIp_->c_str()); 
            servaddr.sin_port = htons(serverPort_);

            if (connect(serverSocket_, (struct sockaddr *)&servaddr, sizeof(servaddr)) < 0) {
                assert("Could not connect to server." && false);
            }
            running = true;
            listenToServerThread = new std::thread(&Node::listenToServer, this);
            sendToServer(getRegistrationMessage());
        }

        //initializes peer to peer listening for other Nodes
        void initializePeerToPeer() {
            int opt = 1;
            clientSocket_ = socket(AF_INET, SOCK_STREAM, 0);
            if (clientSocket_ < 0) {
                assert("Error creating socket." && false);
            }
            //Allows multiple connections on a socker
            if( setsockopt(clientSocket_, SOL_SOCKET, SO_REUSEADDR, (char*)&opt, sizeof(opt)) < 0) {
                assert("Error allowing multiple connections." && false);
            }

            clientaddr.sin_family = AF_INET;
            clientaddr.sin_addr.s_addr = inet_addr(ip_->c_str());
            clientaddr.sin_port = htons(port_);

            //bind to user provided client port for listening to neighbors
            if (::bind(clientSocket_, (struct sockaddr *)&clientaddr, sizeof(clientaddr)) < 0) { 
                assert("Error binding client socket." && false);
            }

            //Attempt to make non-blocking
            if (ioctl(clientSocket_, FIONBIO, (char*)&opt) < 0) {
                assert("Failure setting to nonblocking." && false);
            }

            //Setup to listen to other nodes
            if (listen(clientSocket_, TEMP_CLIENTS_MAX - 1) < 0) {
                assert("Failed to listen." && false);
            }
            p("Listening to neighbors on port ").pln(port_);
        }

        //listens to incoming and active Node connections
        void listenToNeighbors() {
            initializePeerToPeer();

            FD_ZERO(&neighborCurrentFds);
            FD_SET(clientSocket_, &neighborCurrentFds);

            while (running) {
                neighborReadFds = neighborCurrentFds;
                if (select(FD_SETSIZE, &neighborReadFds, NULL, NULL, NULL) < 0) {
                    assert("Error selecting." && false);
                }
                for (int i = 0; i < FD_SETSIZE; i++) {
                    if (FD_ISSET(i, &neighborReadFds)) {
                        if (i == clientSocket_) {
                            //accept incoming connection
                            int addrlen = sizeof(clientaddr);
                            int new_socket;
                            if ((new_socket = accept(clientSocket_,(struct sockaddr*)&clientaddr, (socklen_t*)&clientaddr)) < 0) {
                                 assert("Error accepting new socket." && false);
                            }
                            FD_SET(new_socket, &neighborCurrentFds);
                        } else {
                            handleNodeMsg(i, readIncomingNodeMsg(i));
                        }
                    }
                }
            }
        }

        //reads the incoming msg from the given file descriptor
        unsigned char* readIncomingNodeMsg(int fd) {
            //clear buffer
            memset(neighborBuffer, 0, BUFF_SIZE);
            int bytesRead = read(fd, neighborBuffer, BUFF_SIZE);
            if (bytesRead < 0) {
                assert("Error reading incoming data." && false);
            }
            unsigned char* newBuff = new unsigned char[BUFF_SIZE];
            memcpy(newBuff, neighborBuffer, BUFF_SIZE);
            //clear buffer
            memset(neighborBuffer, 0, BUFF_SIZE);
            return newBuff;
        }

        void handleDisconnect(int fd) {
            close(fd);
            FD_CLR(fd, &neighborCurrentFds);
        }

        //handles messages from other Nodes
        void handleNodeMsg(int fd, unsigned char* msg) {
            if (*msg != 0) {
                MsgKind kind = message_kind(msg);
                switch (kind) {
                    case MsgKind::Status: {
                        handleStatus(fd, msg);
                        break;
                    }
                    default: {
                        assert("Unrecognized message" && false);
                    }
                }
            } else {
                handleDisconnect(fd);
            }
        }

        void sendToNeighbor(int fd, unsigned char* msg) {
            if (send(fd, msg, message_length(msg), 0) < 0) {
                assert("Error sending data to neighbor node." && false);
            }
        }

        //handler for status messages
        void handleStatus(int fd, unsigned char* msg) {
            Status* incomingStatus = new Status(msg);
            p("Received on ").p(ip_->c_str()).p(":").p(port_).p(": ").pln(incomingStatus->msg_->c_str());
            delete incomingStatus;
        }

        //listens to the server for directory updates
        void listenToServer() {
            while (running) {
                memset(serverBuffer, 0, BUFF_SIZE);
                read(serverSocket_, serverBuffer, BUFF_SIZE);
                handleIncoming(serverBuffer);
            }
        }

        //handles incoming messages from the server
        void handleIncoming(unsigned char* data) {
            MsgKind kind = message_kind(data);
            switch (kind) {
                case MsgKind::Ack: {
                    Ack* a = new Ack(data);
                    handleAck(a);
                    break;
                }    
                case MsgKind::Nack: {
                    Nack* n = new Nack(data);
                    handleNack(n);
                    break;
                }
                case MsgKind::Directory: {
                    updateConnections(data);
                    break;
                }
                case MsgKind::Kill: {
                    shutdown();
                    break;
                }
            }
        }

        //shuts down the node
        void shutdown() {
            running = false;
            closeNodeConnections();
            closeServerConnection();
            listenToNeighborsThread->join();
            listenToServerThread->join();
            pln("Gracefully exited.");
            exit(0);
        }

        //closes all active connections with other Nodes
        void closeNodeConnections() {
            for (int i = 0; i < nodeDir->ports_len_; i++) {
                close(neighborSockets[i]);
            }
        }

        //closes the connection with the rendesvouz server
        void closeServerConnection() {
            close(serverSocket_);
        }

        //updated the node directory and opens connections with all other nodes
        void updateConnections(unsigned char* data) {
            nodeDir = new Directory(data);
            createNeighborConnections();
            greetAllNeighbors();
        }

        //Creates connections with all other nodes in the node directory
        void createNeighborConnections() {
            for (int i = 0; i < nodeDir->ports_len_; i++) {
                if (neighborSockets[i] == NULL) {
                    if (!(nodeDir->addresses->vals_[i]->equals(ip_) && nodeDir->ports[i] == port_)) {
                        neighborSockets[i] = socket(AF_INET, SOCK_STREAM, 0);
                        if (neighborSockets[i] < 0) {
                            assert("Error creating socket." && false);
                        }
                        struct sockaddr_in neighboraddr;
                        neighboraddr.sin_family = AF_INET;
                        neighboraddr.sin_addr.s_addr = inet_addr(ip_->c_str());
                        neighboraddr.sin_port = htons(port_);
                        if (connect(neighborSockets[i], (struct sockaddr *)&neighboraddr, sizeof(neighboraddr)) < 0) {
                            assert("Could not connect to neighbor." && false);
                        }
                    }
                }
            }
        }

        //greets all neighbors within the node directory with a status message
        //Proof of concept for MVP
        void greetAllNeighbors() {
            for (int i = 0; i < nodeDir->ports_len_; i++) {
                if (neighborSockets[i] != NULL) {
                    StrBuff* sb = new StrBuff();
                    sb->c("Hello from ");
                    sb->c(ip_->c_str());
                    sb->c(":");
                    sb->c(port_);
                    Status* greetStatus = new Status(sb->get());
                    sendToNeighbor(neighborSockets[i], greetStatus->serialize());
                    delete sb;
                    delete greetStatus;
                }
            }
        }

        //handler for Ack messages
        void handleAck(Ack* ack) {
            switch (ack->previous_kind) {
                case MsgKind::Register: {
                    pln("Successfully registered!");
                    if (listenToNeighborsThread == nullptr) {
                        //launch thread to listen to neighbors
                        listenToNeighborsThread = new std::thread(&Node::listenToNeighbors, this);
                    }
                    break;
                }
                default: {
                    assert("Not implemented yet" && false);
                }
            }
        }

        //handler for Nack messages
        void handleNack(Nack* nack) {
            switch (nack->previous_kind) {
                case MsgKind::Register: {
                    assert("Registration failed." && false);
                    break;
                }
                default: {
                    assert("Not implemented yet" && false);
                }
            }
        }

        //creates a serialized registration message from this Node
        unsigned char* getRegistrationMessage() {
            Register* rMsg = new Register(port_, ip_);
            return rMsg->serialize();
        }
};