//CwC
#pragma once 
#include "../../string.h"
#include "array.h"
#include "serial.h"

enum class MsgKind { 
    Ack = 'A', 
    Nack = 'N', 
    Put = 'P',
    Reply = 'R',  
    Get = 'G', 
    WaitAndGet = 'W', 
    Status = 'S', 
    Kill = 'K',   
    Register = 'R',  
    Directory = 'D' 
};

class Message : public Object, public Serializable  {
    public:
        MsgKind kind_;  // the message kind
        size_t sender_; // the index of the sender node
        size_t target_; // the index of the receiver node
        size_t id_;     // an id t unique within the node

        /** Serializes this Message, structure is as follows:
         * |--1 byte-|--8 bytes----|--8 bytes each--|--8 bytes--|
         * |--type---|--sender_----|--target_-------|--id_------|
         */
        unsigned char* serialize() {
            unsigned char* buffer = new unsigned char[25];
            buffer[0] = serialize_msg_kind(kind_);
            insert_size_t(sender_, buffer, 1);
            insert_size_t(target_, buffer, 9);
            insert_size_t(id_, buffer, 17);
            return buffer;
        }

        /** Deserialize, mutating this object to match the buffer */
        size_t deserialize(unsigned char* buffer) {
            kind_ = extract_msg_kind(buffer, 0);
            sender_ = extract_size_t(buffer, 1);
            target_ = extract_size_t(buffer, 9);
            id_ = extract_size_t(buffer, 17);
            return 25;
        }

        bool equals(Object* other) {
            if (other == this) return true;
            Message* x = dynamic_cast<Message*>(other);
            if (x == nullptr) return false;
            if (kind_ != x->kind_ || sender_!= x->sender_ || target_!= x->target_ || id_!= x->id_) return false;
            return true;
        }

};

class Ack : public Message {
    public:
        MsgKind previous_kind;

        Ack() {
            kind_ = MsgKind::Ack;
        }

        Ack(MsgKind prev_kind) : Ack() {
            previous_kind = prev_kind;
        }

        Ack(size_t sender, size_t target, size_t id, MsgKind prev_kind) : Ack(prev_kind) {
            sender_ = sender;
            target_ = target;
            id_ = id;
        }

        Ack(unsigned char* buffer) {
            deserialize(buffer);
        }

        /** Serializes this Ack, structure is as follows:
         * |--8 bytes------|--25 bytes----------|--1 byte---------|
         * |--Total bytes--|--Message data------|--previous_type--|
         */
        unsigned char* serialize() {
            unsigned char* buffer = new unsigned char[34];
            insert_size_t(34, buffer, 0);
            unsigned char* temp_buffer = Message::serialize();
            copy_unsigned(buffer + 8, temp_buffer, 25);
            delete temp_buffer;
            buffer[33] = serialize_msg_kind(previous_kind);
            return buffer;
        }

        /** Deserialize, mutating this object to match the buffer */
        size_t deserialize(unsigned char* buffer) {
            size_t msg_size = Message::deserialize(buffer + 8);
            previous_kind = extract_msg_kind(buffer, 33);
            return extract_size_t(buffer, 0);
        }

        bool equals(Object* other) {
            if (other == this) return true;
            Ack* x = dynamic_cast<Ack*>(other);
            if (x == nullptr) return false;
            if (previous_kind != x->previous_kind) return false;
            return Message::equals(other);
        }
};

class Nack : public Message {
    public:
        MsgKind previous_kind;

        Nack() {
            kind_ = MsgKind::Nack;
        }

        Nack(MsgKind prev_kind) : Nack() {
            previous_kind = prev_kind;
        }

        Nack(size_t sender, size_t target, size_t id, MsgKind prev_kind) : Nack(prev_kind) {
            sender_ = sender;
            target_ = target;
            id_ = id;
        }

        Nack(unsigned char* buffer) {
            deserialize(buffer);
        }

        /** Serializes this Nack, structure is as follows:
         * |--8 byte-------|--25 bytes----------|--1 byte---------|
         * |--Total bytes--|--Message data------|--previous_type--|
         */
        unsigned char* serialize() {
            unsigned char* buffer = new unsigned char[34];
            insert_size_t(34, buffer, 0);
            unsigned char* temp_buffer = Message::serialize();
            copy_unsigned(buffer + 8, temp_buffer, 25);
            delete temp_buffer;
            buffer[33] = serialize_msg_kind(previous_kind);
            return buffer;
        }

        /** Deserialize, mutating this object to match the buffer */
        size_t deserialize(unsigned char* buffer) {
            size_t msg_size = Message::deserialize(buffer + 8);
            previous_kind = extract_msg_kind(buffer, 33);
            return extract_size_t(buffer, 0);;
        }

        bool equals(Object* other) {
            if (other == this) return true;
            Nack* x = dynamic_cast<Nack*>(other);
            if (x == nullptr) return false;
            if (previous_kind != x->previous_kind) return false;
            return Message::equals(other);
        }
};

class Kill : public Message {
    public:

        Kill() {
            kind_ = MsgKind::Kill;
        }

        Kill(size_t sender, size_t target, size_t id, MsgKind prev_kind) : Kill() {
            sender_ = sender;
            target_ = target;
            id_ = id;
        }

        Kill(unsigned char* buffer) {
            deserialize(buffer);
        }

        /** Serializes this Ack, structure is as follows:
         * |--8 bytes------|--25 bytes----------|
         * |--Total bytes--|--Message data------|
         */
        unsigned char* serialize() {
            unsigned char* buffer = new unsigned char[33];
            insert_size_t(33, buffer, 0);
            unsigned char* temp_buffer = Message::serialize();
            copy_unsigned(buffer + 8, temp_buffer, 25);
            delete temp_buffer;
            return buffer;
        }

        /** Deserialize, mutating this object to match the buffer */
        size_t deserialize(unsigned char* buffer) {
            size_t msg_size = Message::deserialize(buffer + 8);
            return extract_size_t(buffer, 0);;
        }

        bool equals(Object* other) {
            if (other == this) return true;
            Kill* x = dynamic_cast<Kill*>(other);
            if (x == nullptr) return false;
            return Message::equals(other);
        }
};

class Status : public Message {
    public:
        String* msg_; // owned

        Status() {
            kind_ = MsgKind::Status;
        }

        Status(String* msg) : Status() {
            msg_ = msg->clone();
        }

        Status(size_t sender, size_t target, size_t id, String* msg) : Status(msg) {
            sender_ = sender;
            target_ = target;
            id_ = id;
        }

        Status(unsigned char* buffer) {
            deserialize(buffer);
        }

        /** Serializes this Ack, structure is as follows:
         * |--8 bytes------|--25 bytes----------|--unknown bytes--|
         * |--Total bytes--|--Message data------|--msg_-----------|
         */
        unsigned char* serialize() {
            size_t length = 8 + 25 + strlen(msg_->c_str()) + 1;
            unsigned char* buffer = new unsigned char[length];
            insert_size_t(length, buffer, 0);
            unsigned char* temp_buffer = Message::serialize();
            copy_unsigned(buffer + 8, temp_buffer, 25);
            delete temp_buffer;
            insert_string(msg_, buffer, 33);
            buffer[length - 1] = '\0';
            return buffer;
        }

        /** Deserialize, mutating this object to match the buffer */
        size_t deserialize(unsigned char* buffer) {
            size_t msg_size = Message::deserialize(buffer + 8);
            msg_ = extract_string(buffer, 33);
            return extract_size_t(buffer, 0);;
        }

        bool equals(Object* other) {
            if (other == this) return true;
            Status* x = dynamic_cast<Status*>(other);
            if (x == nullptr) return false;
            if (!msg_->equals(x->msg_)) return false;
            return Message::equals(other);
        }
};

class Register : public Message {
    public:
        size_t port; 
        String* IP;

        Register() {
            kind_ = MsgKind::Register;
        }

        Register(size_t prt, String* ip) : Register() {
            port = prt;
            IP = ip->clone();
        }

        Register(size_t sender, size_t target, size_t id, size_t prt, String* ip) : Register(prt, ip) {
            sender_ = sender;
            target_ = target;
            id_ = id;
        }

        Register(unsigned char* buffer) {
            deserialize(buffer);
        }

        /** Serializes this Ack, structure is as follows:
         * |--8 bytes-----|--25 bytes----------|--8 bytes--|--unknown bytes--|
         * |--Total size--|--Message data------|--port-----|--msg_-----------|
         */
        unsigned char* serialize() {
            size_t length = 8 + 25 + 8 + strlen(IP->c_str()) + 1;
            unsigned char* buffer = new unsigned char[length];
            insert_size_t(length, buffer, 0);
            unsigned char* temp_buffer = Message::serialize();
            copy_unsigned(buffer + 8, temp_buffer, 25);
            delete temp_buffer;
            insert_size_t(port, buffer, 33);
            insert_string(IP, buffer, 41);
            buffer[length - 1] = '\0';
            return buffer;
        }

        /** Deserialize, mutating this object to match the buffer */
        size_t deserialize(unsigned char* buffer) {
            size_t msg_size = Message::deserialize(buffer + 8);
            port = extract_size_t(buffer, 33);
            IP = extract_string(buffer, 41);
            return extract_size_t(buffer, 0);;
        }

        bool equals(Object* other) {
            if (other == this) return true;
            Register* x = dynamic_cast<Register*>(other);
            if (x == nullptr) return false;
            if (!IP->equals(x->IP) || port != x->port) return false;
            return Message::equals(other);
        }
};

class Directory : public Message {
    public:
        size_t client;
        size_t ports_len_;
        size_t ports_cap_;
        size_t * ports;  // owned
        StringArray* addresses;  // owned; strings owned

        Directory() {
            kind_ = MsgKind::Directory;
            client = 0;
            ports_len_ = 0;
            ports_cap_ = 4;
            ports = new size_t[4];
            addresses = new StringArray(4);
        }

        Directory(size_t maxNodes) {
            kind_ = MsgKind::Directory;
            client = 0;
            ports = new size_t[maxNodes];
            addresses = new StringArray(maxNodes);
            ports_len_ = 0;
            ports_cap_ = maxNodes;
        }

        Directory(unsigned char * buffer) {
            deserialize(buffer);
        }

        void push_port(size_t s) {
            ports[ports_len_] = s;
            ports_len_++;
        }

        bool addNode(String* ip, size_t port) {
            if (addresses->can_push() && ports_len_ < ports_cap_ && !contains_node(ip, port)) {
                addresses->push(ip);
                push_port(port);
                return true;
            }
            return false;
        }

        bool contains_node(String* ip, size_t port) {
            for (size_t i = 0; i < ports_len_; i++)
                if (ports[i] == port && ip->equals(addresses->vals_[i])) return true;
            return false;
        }

        void removeNode(String* ip, size_t port) {
            int index = addresses->index_of(ip);
            if (index < 0) {
                return;
            } else {
                addresses->remove(index);
                collapse(index);
                ports_len_--;
            }
        }

        void collapse(size_t i) {
            for (size_t j = i; j < ports_len_ - 1; j++) {
                ports[j] = ports[j + 1];
            }
            ports[ports_len_ - 1] = NULL;
        }

        void print() {
            pln("NODE DIR:");
            for (size_t i = 0; i < ports_cap_; i++) {
                if (addresses->vals_[i] != nullptr) {
                    StrBuff* sb = new StrBuff();
                    sb->c(addresses->vals_[i]->c_str());
                    sb->c(":");
                    sb->c(ports[i]);
                    pln(sb->get()->c_str());
                    delete sb;
                }
            }
        }

        /** Serializes this Directory, structure is as follows:
         * |--8 bytes-----|--25 bytes--|--8 bytes-|--8 bytes----|-8 bytes each----|--variable, handled by StringArray()--|
         * |--Toal bytes--|--Message---|--client--|--ports_len--|--ports 1, 2...--|--addresses---------------------------|
         */
        unsigned char* serialize() {
            //Calculate length needed
            unsigned char* address = addresses->serialize();
            size_t length = 8 + 25 + 8 + 8 + 8 * ports_len_ + extract_size_t(address, 0);
            //Init buffer, insert starting size_t's of the buffer
            unsigned char* buffer = new unsigned char[length];
            insert_size_t(length, buffer, 0);
            unsigned char* temp_buffer = Message::serialize();
            copy_unsigned(buffer + 8, temp_buffer, 25);
            delete temp_buffer;
            insert_size_t(client, buffer, 33);
            insert_size_t(ports_len_, buffer, 41);
            //Insert ports
            for (size_t i = 0; i < ports_len_; i++)
                insert_size_t(ports[i], buffer, 24 + 25 + 8 * i);
            //Copy in StringArray
            copy_unsigned(buffer + 24 + 25 + 8 * ports_len_, address, extract_size_t(address, 0));
            return buffer;
        }

        /** Deserialize, mutating this object to match the buffer */
        size_t deserialize(unsigned char* buffer) {
            size_t msg_size = Message::deserialize(buffer + 8);
            client = extract_size_t(buffer, 8 + 25);
            ports_len_ = extract_size_t(buffer, 16 + 25);
            ports_cap_ = ports_len_ * 2;
            ports = new size_t[ports_cap_];
            for (size_t i = 0; i < ports_len_; i++)
                ports[i] = extract_size_t(buffer, 24 + 25 + 8 * i);
            addresses = new StringArray();
            size_t arr_size = addresses->deserialize(buffer + 24 + 25 + 8 * ports_len_);
            return extract_size_t(buffer, 0);
        }

        bool equals(Object* other) {
            if (other == this) return true;
            Directory* x = dynamic_cast<Directory*>(other);
            if (x == nullptr) return false;
            if (ports_len_ != x->ports_len_ || client!= x->client) return false;
            if (!addresses->equals(x->addresses)) return false;
            for (size_t i = 0; i < ports_len_; i++) {
                if (ports[i] != x->ports[i]) return false;
            }
            return Message::equals(other);
        }
};