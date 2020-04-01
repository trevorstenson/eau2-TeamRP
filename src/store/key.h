//lang: cwc

#pragma once

#include "../object.h"
#include "../string.h"

class Key : public Object {
    public:
        String* name_;
        size_t node_;

        Key(const char* name, size_t node) {
            name_ = new String(name);
            node_ = node;
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
};