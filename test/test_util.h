#pragma once

#include <iostream>

void success(const char* test_name) {
    std::cout << "\033[1;32mSUCCESS \033[0m" << test_name << std::endl;
}