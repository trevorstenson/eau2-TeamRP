//lang: cwc

#pragma once

#include "../object.h"
#include "../string.h"
#include "../serial/serial.h"

class Key : public Object, public Serializable {
    public:
        String* name_;
        size_t node_;

        Key() { }

        Key(const char* name, size_t node) {
            name_ = new String(name);
            node_ = node;
        }

        Key(unsigned char* serial) {
            deserialize(serial);
        }

        ~Key() {
            delete name_;
        }

        bool equals(Object  * other) {
            if (this == other) return true;
            Key* o = dynamic_cast<Key*>(other);
            if (o == nullptr) return false;
            return name_->equals(o->name_) && node_ == o->node_;
        }

        size_t hash_me() {
            return name_->hash() + node_ + 2;
        }

        unsigned char *serialize() {
            size_t length = 8 + 8 + strlen(name_->c_str()) + 1;
            unsigned char* buffer = new unsigned char[length];
            insert_size_t(length, buffer, 0);
            insert_size_t(node_, buffer, 8);
            insert_string(name_, buffer, 16);
            buffer[length - 1] = '\0';
            return buffer;
        }

        size_t deserialize(unsigned char *serialized) {
            size_t length = extract_size_t(serialized, 0);
            node_ = extract_size_t(serialized, 8);
            name_ = extract_string(serialized, 16);
            assert(length == 17 + strlen(name_->c_str()));
            return length;
        }
};