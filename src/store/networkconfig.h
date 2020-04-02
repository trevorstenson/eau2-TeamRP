#include "../object.h"
#include "../string.h"
#include <unistd.h>
#include <arpa/inet.h>
#include <sys/types.h>  
#include <sys/socket.h>  
#include <netinet/in.h>  
#include <sys/time.h>
#include <sys/ioctl.h>

#define TEMP_CLIENTS_MAX 30

class NetworkConfig : public Object {
    public:
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

        NetworkConfig() {
            ip_ = nullptr;
            serverIp_ = nullptr;
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

        ~NetworkConfig() {
            delete ip_;
            delete serverIp_;
            delete[] neighborSockets;
            delete nodeDir;
            delete[] serverBuffer;
            delete[] neighborBuffer;
            delete listenToNeighborsThread;
            delete listenToServerThread;
        }
};