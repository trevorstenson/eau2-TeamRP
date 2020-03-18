//CwC
#pragma once 
#include "../../string.h"
#include "utils.h"
#include "serial.h"

class Serializable {
    public: 
        virtual unsigned char* serialize() { };
        virtual void deserialize(unsigned char* serialized) { };

        virtual ~Serializable() { };
};

//Serialization utility functions to make serialization easier

/** Serialize the size_t */
unsigned char* serialize_size_t(size_t s) {
    //Initialize new array to hold bytes
    unsigned char* serialization = new unsigned char[8];
    //Convert to byte array 
    for (int i = 0; i < 8; i++)
       serialization[i] = (s >> (i * 8));
    return serialization;
}

/** Insert the size_t into the buffer at a given offset */
void insert_size_t(size_t s, unsigned char* buffer, size_t offset) {
    unsigned char* char_to_insert = serialize_size_t(s);
    copy(buffer + offset, char_to_insert, 8);
    delete char_to_insert;
}

/** Extract a size_t from a buffer at the given offset */
size_t extract_size_t(unsigned char* buffer, size_t offset) {
    size_t val = 0;
    //Bitwise or on bytes. Shifted to correct position
    for (size_t i = 0; i < 8; i++) 
        val = val | (buffer[i + offset] << (i * 8));
    return val;
}

size_t deserialize_size_t(unsigned char* buffer) {
    return extract_size_t(buffer, 0);
}

//Utility functions to make serialization easier
unsigned char* serialize_double(double d) {
    unsigned char* pointer = reinterpret_cast<unsigned char*>(&d);
    unsigned char* buffer = new unsigned char[8];
    for (size_t i = 0; i < 8; i++) {
        buffer[i] = pointer[i];
    }
    return buffer;
}

/** Insert the size_t into the buffer at a given offset */
void insert_double(double d, unsigned char* buffer, size_t offset) {
    unsigned char* char_to_insert = serialize_double(d);
    copy(buffer + offset, char_to_insert, 8);
    delete char_to_insert;
}

double extract_double(unsigned char* buffer, size_t offset) {
    return *reinterpret_cast<double*>(buffer + offset);
}

double deserialize_double(unsigned char* buffer) {
    return extract_double(buffer, 0);
}

/** Serialize the String* */
unsigned char* serialize_string(String* s) {
    return reinterpret_cast<unsigned char*>(strdup(s->c_str()));
}

/** Insert the String into the buffer at a given offset, 
 * returns the index after the insert string in the buffer */
size_t insert_string(String* s, unsigned char* buffer, size_t offset) {
    unsigned char* temp = serialize_string(s);
    size_t length = strlen(reinterpret_cast<char*>(temp));
    copy(buffer + offset, temp, length);
    return offset + length;
}

/** Extract a size_t from a buffer at the given offset */
String* extract_string(unsigned char* buffer, size_t offset) {
    return new String(reinterpret_cast<char*>(buffer + offset));
}

String* deserialize_string(unsigned char* buffer) {
    return extract_string(buffer, 0);
}

//Stub out MsgKind to avoid circular dependencies
enum class MsgKind;

char serialize_msg_kind(MsgKind m) {
    return (char)m;
}

MsgKind deserialize_msg_kind(unsigned char s) {
    return MsgKind(s);
}

MsgKind extract_msg_kind(unsigned char* buffer, size_t offset) {
    return deserialize_msg_kind(buffer[offset]);
}

size_t message_length(unsigned char* buffer) {
    return extract_size_t(buffer, 0);
}

MsgKind message_kind(unsigned char* buffer) {
    return deserialize_msg_kind(buffer[8]);
}

                