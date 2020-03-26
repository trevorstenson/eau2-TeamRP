#pragma once
#include "object.h"
#include "assert.h"
#include "string.h"
#include "store/key.h"
#include "store/value.h"
#include <stdlib.h>
#include <iostream>

/**
 * Class to hold a mapping of a key to a value. To be used by Map class
 */
class Pair: public Object {
    public:
        Object* key_;
        Object* value_;

        /**
         * Default constructor of the Pair class
         * @param key the Pair's key
         * @param value the Pair's value
         */
        Pair(Object* key, Object* value) : Object() {
            key_ = key;
            value_ = value;
        }
        
        /**
         * Default destructor for Pair
         */
        ~Pair() {
            delete key_;
            delete value_;
        }

        /**
         * @return The key associated with this pair
         */
        Object* getKey() {
            return key_;
        }

        /**
         * @return The value associated with this pair
         */
        Object* getValue() {
            return value_;
        }

        /**
         * Change the value of the pair to the input value
         * @param value The object that will replace the current value
         * @return The object being replaced
         */
        Object* setValue(Object* value) {
            Object* temp = value_;
            value_ = value;
            return temp;
        }   

        /**
         * @brief - Get the hash of this Pair.
         * 
         * @return size_t - the hash of this Pair
         */
        size_t hash_me() {
            return key_->hash() + value_->hash();
        }
};

/**
 * Map - data structure that is to be used for our project which maps an Object to an Object
 */ 
class Map: public Object {
    public:

        Pair** values_;
        size_t len_;
        size_t capacity_;

        /**
         * @brief Basic initialization of an empty Map
         */
        Map() {
            len_ = 0;
            capacity_ = 0;
            values_ = new Pair*[capacity_];
        }

        /**
         * @brief Deletes a Map including all private data structures, such as buckets
         * NOTE: Objects inside the Map will NOT be freed
         */
        ~Map() {
            delete[] values_;
        }

        /**
         * @brief - Get the size of this map.
         * 
         * @return size_t - the size of this map.
         */
        virtual size_t size() {
            return len_;
        }

        /**
         * @brief - Is this map empty?
         * 
         * @return true - if this map is empty
         * @return false: if this map is not empty
         */
        virtual bool isEmpty() {
            return len_ == 0;
        }

        /**
         * @brief - Does this map contain key?
         * 
         * @param key - the key to search for
         * @return true - if the key exists in this map
         * @return false - if the key does not exist in this map
         */
        virtual bool containsKey(Object* key) {
            for (size_t i = 0; i < len_; i++) {
                if (values_[i]->getKey()->equals(key)) {
                    return true;
                }
            }
            return false;
        }

        /**
         * @brief - Does this map contain value?
         * 
         * @param value - the value to search for
         * @return true - if the value exists in this map
         * @return false - if the value does not exist in this map
         */
        virtual bool containsValue(Object* value) {
            for (size_t i = 0; i < len_; i++) {
                if (values_[i]->getValue()->equals(value)) {
                    return true;
                }
            }
            return false;
        }

        /**
         * @brief - Get the value for the key.
         * If the key does not exist, return a nullptr.
         * 
         * @param key - the key to return the value for.
         * @return Object* - the value that corresponds to key
         */
        virtual Object* get(Object* key) {
            for (size_t i = 0; i < len_; i++) {
                if (values_[i]->getKey()->equals(key)) {
                    return values_[i]->getValue();
                }
            }
            return nullptr;
        }
        
        /**
         * @brief - Put the given key-value pair in this map.
         * 
         * @param key - the key to insert, cannot be null
         * @param value - the value to insert
         * @return Object* - the previous value for the given key
         */
        virtual Object* put(Object* key, Object* value) {
            if (key == NULL) {
                assert(("Given key cannot be null.", false));
            }
            if (containsKey(key)) {
                for (size_t i = 0; i < len_; i++) {
                    if (values_[i]->getKey()->equals(key)) {
                        return values_[i]->setValue(value);
                    }
                }
            } else {
                ensureCapacity();
                Pair* pair = new Pair(key, value);
                values_[len_] = pair;
                len_++;
                return nullptr;
            }
        }

        /**
         * @brief - Remove the key-value pair from this map.
         * 
         * @param key - the key to remove
         * @return Object* - the value of the key that was removed
         */
        virtual Object* remove(Object* key) {
            for (size_t i = 0; i < len_; i++) {
                if (values_[i]->getKey()->equals(key)) {
                    Object* removedObject = values_[i]->getValue();
                    shiftPairs(i);
                    len_--;
                    return removedObject;
                }
            }
            return NULL;
        }

        void shiftPairs(size_t index) {
            for (size_t i = index; i < len_ - 1; i++) {
                values_[i] = values_[i + 1];
            }
        }

        /**
         *  @brief - ensures the internal array of Pairs grows if it needs to
         */
        void ensureCapacity() {
            if (len_ == capacity_) {
                grow();
            }
        }

        /**
         *  @brief - Doubles the size of the internal array of Pairs
         */
        void grow() {
            if (capacity_ == 0) {
                capacity_++;
            } else {
                capacity_ = capacity_ * 2;
            }
            Pair** newValues = new Pair*[capacity_];
            for (size_t i = 0; i < len_; i++) {
                newValues[i] = values_[i];
            }
            delete[] values_;
            values_ = newValues;
        }

        /**
         * @brief - Put all the contents of other into this map.
         * 
         * @param other - the other map to load all the contents from
         */
        //TODO impl this with keyset and valueset
        virtual void putAll(Map* other) {
            for (size_t i = 0; i < other->size(); i++) {
                Pair* pair = other->values_[i];
                put(pair->getKey(), pair->getValue());
            }
        }

        /**
         * @brief - Clear the contents of this map so it is empty.
         * 
         */
        virtual void clear() {
            delete[] values_;
            len_ = 0;
        }

        /**
         * @brief - Does this map equal other?
         *  
         * @param other - the object to compare this map to
         * @return true - if this map equals other
         * @return false - if this map does not equal other
         */
        // TODO - implement using Array Keyset and values
        virtual bool equals(Object* other) {
            if (other == this) return true;
            Map* otherMap = dynamic_cast<Map*>(other);
            if (otherMap == nullptr) return false;
            if (otherMap->size() != len_) return false;
            return hash() == otherMap->hash();
        }

        /**
         * @brief - Get the hash of this map.
         * 
         * @return size_t - the hash of this map
         */
        virtual size_t hash_me() {
            size_t hash = 81237;
            for (size_t i = 0; i < len_; i++) {
                hash += values_[i]->hash();
            }
            return hash;
        }
};

class KVMap : public Map {
    public:
        KVMap() : Map() { }

        virtual bool containsKey(Key* key) {
            for (size_t i = 0; i < len_; i++) {
                Key* k = dynamic_cast<Key*>(values_[i]->getKey());
                if (k != nullptr && k->equals(key)) {
                    return true;
                }
            }
            return false;
        }

        virtual bool containsValue(Value* value) {
            for (size_t i = 0; i < len_; i++) {
                Value* v = dynamic_cast<Value*>(values_[i]->getValue());
                if (v != nullptr && v->equals(value)) {
                    return true;
                }
            }
            return false;
        }

        virtual Value* get(Key* key) {
            for (size_t i = 0; i < len_; i++) {
                Key* k = dynamic_cast<Key*>(values_[i]->getKey());
                if (k != nullptr && k->equals(key)) {
                    return dynamic_cast<Value*>(values_[i]->getValue());
                }
            }
            return nullptr;
        }

        virtual Value* put(Key* key, Value* value) {
            if (key == nullptr) {
                assert(("Given key cannot be null.", false));
            }
            if (containsKey(key)) {
                for (size_t i = 0; i < len_; i++) {
                    Key* k = dynamic_cast<Key*>(values_[i]->getKey());
                    if (k != nullptr && k->equals(key)) {
                        return dynamic_cast<Value*>(values_[i]->setValue(value));
                    }
                }
            } else {
                ensureCapacity();
                Pair* pair = new Pair(key, value);
                values_[len_] = pair;
                len_++;
                return nullptr;
            }
        }

        virtual Value* remove(Key* key) {
            return dynamic_cast<Value*>(Map::remove(key));
        }
};

/**
 * @brief OSMap - Map from Object to String
 * 
 */
class OSMap : public Map {
    public:
        OSMap(): Map() { }

                /**
         * @brief - Does this map contain value?
         * 
         * @param value - the value to search for
         * @return true - if the value exists in this map
         * @return false - if the value does not exist in this map
         */
        bool containsValue(String* value) {
            for (size_t i = 0; i < len_; i++) {
                String* val = dynamic_cast<String*>(values_[i]->getValue());
                if (val->equals(value)) return true;
            }
            return false;
        }

        /**
         * @brief - Get the value for the key.
         * If the key does not exist, return a nullptr.
         * 
         * @param key - the key to return the value for.
         * @return String* - the value that corresponds to key
         */
        virtual String* get(Object* key) {
            if (!containsKey(key)) return nullptr;
            return dynamic_cast<String*>(Map::get(key));
        }

        /**
         * @brief - Put the given key-value pair in this map.
         * 
         * @param key - the key to insert
         * @param value - the value to insert
         * @return String* - the previous value for the given key
         */
        virtual String* put(Object* key, String* value) {
            Map::put(key, value);
        }

        /**
         * @brief - Remove the key-value pair from this map.
         * 
         * @param key - the key to remove
         * @return String* - the value of the key that was removed if exists, else nullptr
         */
        virtual String* remove(Object* key) {
            return dynamic_cast<String*>(Map::remove(key));
        }
};