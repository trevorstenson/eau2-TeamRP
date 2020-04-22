//lang: cwc

#pragma once

#include "../object.h"
#include "../serial/serial.h"

class Value : public Object {
    public:
        size_t blob_length_;
        unsigned char* blob_;

        Value() { }

        Value(unsigned char* serial) {
            deserialize(serial);
        }

        Value(unsigned char* blob, size_t blob_length) {
            blob_length_ = blob_length;
            blob_ = blob;
        }

        ~Value() {
            delete blob_;
        }

        /**
         * Serializes this value. Structure is as follows:
         * |--8 bytes--------------|-8 bytes------|-X bytes-|
         * |-Total length in bytes-|-blob_length_-|-blob_---|
         */
        unsigned char *serialize() {
            //TODO this cast may need to be changed
            size_t length = 16 + blob_length_;
            unsigned char* buffer = new unsigned char[length];
            insert_size_t(length, buffer, 0);
            insert_size_t(blob_length_, buffer, 8);
            copy_unsigned(buffer + 16, blob_, blob_length_);
            return buffer;
        }

        size_t deserialize(unsigned char *serialized) {
            size_t length = extract_size_t(serialized, 0);
            blob_length_ = extract_size_t(serialized, 8);
            blob_ = new unsigned char[blob_length_];
            copy_unsigned(blob_, serialized + 16, blob_length_);
            assert(length == 16 + blob_length_);
            return length;
        }

        bool equals(Object  * other) {
            if (this == other) return true;
            Value* o = dynamic_cast<Value*>(other);
            if (o == nullptr) return false;
            if (blob_length_ != o->blob_length_) return false;
            return (memcmp(blob_, o->blob_, blob_length_) == 0);
        }
};