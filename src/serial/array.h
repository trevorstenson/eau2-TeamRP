//CwC
#pragma once 
#include "../string.h"
#include "serial.h"
#include "../dataframe/column.h"
class StringArray : public Object, public Serializable {
    public:
        size_t len_;
        size_t cap_;
        String** vals_;

        StringArray(size_t capacity) {
            len_ = 0;
            cap_ = capacity;
            vals_ = new String*[cap_];
            for (int i = 0; i < cap_; i++) {
                vals_[i] = nullptr;
            }
        }

        StringArray() : StringArray(30) {}

        ~StringArray() {
            for (size_t i = 0; i < len_; i++) {
                delete vals_[i];
            }
            delete[] vals_;
        }

        StringArray(Column* c): StringArray(c->size()) {
            if (c->get_type() != 'S') assert("Type other than S found." && false);
            StringColumn* b = c->as_string();
            for (size_t i = 0; i < b->size(); i++)
                push(b->get(i));
        }

        /** Serializes the StringArray into an unsigned char array. Structure is as following
         * 
         * |---------8 bytes-----------------|------Unknown length-----|-----Unknown Length------|...
         * |--total serialized array length--|--String 1 c_str()-'\0'--|--String 2 c_str()-'\0'--|...
         * 
         * The fact each string is of unknown length and that we must use '\0' character to delimit necessitates the total
         * serialzied array length at the front
         */ 
        unsigned char* serialize() {
            //Determine needed buffer size. Start at 8 to account for total serialized length size_t. 
            size_t buffer_l = 8;
            for (int i = 0; i < len_; i++) {
                unsigned char* temp = serialize_string(vals_[i]);
                buffer_l += strlen(reinterpret_cast<char*>(temp)) + 1;
                delete temp;
            }

            //Initialize array to needed size
            unsigned char* ret = new unsigned char[buffer_l];
            insert_size_t(buffer_l, ret, 0);

            //Serialize and add strings
            size_t index = 8;
            for (size_t i = 0; i < len_; i++) {
                index = insert_string(vals_[i], ret, index);
                ret[index] = '\0';
                index++;
            }
            return ret;
        }

        /** Deserialize the buffer. Mutates this StringArray to match the buffer */
        size_t deserialize(unsigned char* buffer) {
            //Determine bytes to read
            size_t length = extract_size_t(buffer, 0);
            size_t index = 8;
            //Read in until we've passed the readable bytes
            while (index < length) {
                String* temp = extract_string(buffer, index);
                push(temp);
                index += strlen(temp->c_str()) + 1;
            }
            return length;
        }

        bool can_push() {
            return len_ < cap_;
        }

        void push(String* str) {
            ensureCapacity();
            vals_[len_] = str;
            len_++;
        }

        // ensures the internal array grows if it needs to
        void ensureCapacity() {
            if (len_ == cap_) {
                grow();
            }
        }
        
        // grows the internal array by twice its size
        void grow() {
            if (cap_ == 0) {
                cap_++;
            } else {
                cap_ = cap_ * 2;
            }
            String** newValues = new String*[cap_];
            for (size_t i = 0; i < len_; i++) {
                newValues[i] = vals_[i];
            }
            delete[] vals_;
            vals_ = newValues;
        }

        bool remove(String* str) {
            for (size_t i = 0; i < len_; i++) {
                if (vals_[i]->equals(str)) {
                    delete vals_[i];
                    collapse(i);
                    len_--;
                    return true;
                }
            }
            return false;
        }

        bool remove(size_t s) {
            if (vals_[s] != nullptr) {
                delete vals_[s];
                collapse(s);
                len_--;
                return true;
            }
            return false;
        }

        void collapse(size_t i) {
            for (size_t j = i; j < len_ - 1; j++) {
                vals_[j] = vals_[j + 1];
            }
            vals_[len_ - 1] = nullptr;
        }

        int index_of(String* str) {
            for (size_t i = 0; i < len_; i++)
                if (vals_[i]->equals(str)) return i;
            return -1; 
        }

        bool equals(Object* other) {
            if (other == this) return true;
            StringArray* x = dynamic_cast<StringArray*>(other);
            if (x == nullptr) return false;
            if (len_ != x->len_) return false;
            for (size_t i = 0; i < len_; i++) {
                if (!vals_[i]->equals(x->vals_[i])) return false;
            }
            return true;
        }

        void print() {
            for (size_t i = 0; i < cap_; i++) {
                if (vals_[i] == nullptr) {
                    std::cout << "nullptr\n";
                } else {
                    std::cout << vals_[i]->c_str() << std::endl;
                }
            }
        }
};

 

class DoubleArray : public Object, public Serializable  {
    public:
        size_t len_;
        size_t cap_;
        double* vals_;

        DoubleArray(size_t capacity) {
            len_ = 0;
            cap_ = capacity;
            vals_ = new double[cap_];
            for (int i = 0; i < cap_; i++) {
                vals_[i] = 0;
            }
        }

        DoubleArray() : DoubleArray(30) {}

        ~DoubleArray() {
            delete[] vals_;
        }

        DoubleArray(Column* c) : DoubleArray(c->size()) {
            BoolColumn* x;
            IntColumn* y;
            DoubleColumn* z;
            switch (c->get_type()) {
                case 'B':
                    x = c->as_bool();
                    for (size_t i = 0; i < x->size(); i++) {
                        if (x->get(i)) {
                            push(1);
                        } else {
                            push(0);
                        }
                    }
                    break;
                case 'I':
                    y = c->as_int();
                    for (size_t i = 0; i < y->size(); i++) {
                        push(double(y->get(i)));
                    }
                    break;
                case 'D':
                    z = c->as_double();
                    for (size_t i = 0; i < z->size(); i++) {
                        push(z->get(i));
                    }
                    break;
                default:
                    assert("Type other than B, I, or F found." && false);
            }
        }

        /** Serializes the DoubleArray into an unsigned char array. Structure is as following
         * 
         * |--8 bytes-------------|--8 bytes each-----|
         * |--length in bytes-----|--vals1, vals2...--|
         */ 
        unsigned char* serialize() {
            size_t length = 8 + 8 * len_;
            unsigned char* buffer = new unsigned char[length];
            insert_size_t(length, buffer, 0);
            for (size_t i = 0; i < len_; i++) 
                insert_double(vals_[i], buffer, 8 + 8 * i);
            return buffer;
        }

        /** Deserialize the buffer. Mutates this StringArray to match the buffer */
        size_t deserialize(unsigned char* buffer) {
            len_ = (extract_size_t(buffer, 0) - 8) / 8;
            cap_ = len_;
            vals_ = new double[cap_];
            for (int i = 0; i < len_; i++) 
                vals_[i] = extract_double(buffer, 8 + 8 * i); 
            return extract_size_t(buffer, 0);         
        }

        void push(double dbl) {
            ensureCapacity();
            vals_[len_] = dbl;
            len_++;
        }

        // ensures the internal array grows if it needs to
        void ensureCapacity() {
            if (len_ == cap_) {
                grow();
            }
        }
        
        // grows the internal array by twice its size
        void grow() {
            if (cap_ == 0) {
                cap_++;
            } else {
                cap_ = cap_ * 2;
            }
            double* newValues = new double[cap_];
            for (size_t i = 0; i < len_; i++) {
                newValues[i] = vals_[i];
            }
            delete[] vals_;
            vals_ = newValues;
        }

        bool equals(Object* other) {
            if (other == this) return true;
            DoubleArray* x = dynamic_cast<DoubleArray*>(other);
            if (x == nullptr) return false;
            if (len_ != x->len_) return false;
            for (size_t i = 0; i < len_; i++)
                if (vals_[i] != x->vals_[i]) return false;
            return true;
        }
};