//CwC
#pragma once 

void copy_unsigned(unsigned char* destination, unsigned char* source, size_t length) {
    for (size_t i = 0; i < length; i++) {
        destination[i] = source[i];
    }
}

