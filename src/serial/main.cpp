#include "array.h"
#include "message.h"
#include "serial.h"

using namespace std;

int main() {
    //Create a Register message a new node is going to send to the server.
    Register* registr = new Register(10, 1, 12345, 5, new String("1.1.1.1"));
    //Serialize the message
    unsigned char* register_send = registr->serialize();
    //"Send" the message, then deserialize
    Register* received_register = new Register(register_send);
    //Ensure the messages are the same
    assert(registr->equals(received_register));
    //Respond with an Ack
    Ack* ack = new Ack(1, 10, 6, MsgKind::Register);
    //Serialize 
    unsigned char* ack_send = ack->serialize();
    //"Send" the message, then deserialize
    Ack* received_ack = new Ack(ack_send);
    //Ensure ack is the same
    assert(ack->equals(received_ack));
    cout << "New node succesfully registered and received ack.\n"; 

    //Communicate the directory with all nodes
    Directory* directory = new Directory(40);
    //Serialize
    unsigned char* directory_send = directory->serialize();
    //Send, receive, then deserialzie
    Directory* directory_rec = new Directory(directory_send);
    //Ensrue same
    assert(directory->equals(directory_rec));
    //Pretend directory message corrupted, send Nack
    Nack* nack = new Nack(1, 10, 6, MsgKind::Directory);
    //Serialize 
    unsigned char* nack_send = nack->serialize();
    //"Send" the message, then deserialize
    Nack* received_nack = new Nack(nack_send);
    //Ensure ack is the same
    assert(nack->equals(received_nack));
    cout << "Directory corrupted. Responded with Nack.\n"; 
}