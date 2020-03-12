#pragma once

/**
 * Keeps track of information regarding the current
 * state of parsing within the .sor file
 */
struct ParseState {
    public: 
        char ch;
        bool inField = false;
        bool inQuotes = false;
        unsigned int bytesRead = 0;
        int lineCount = 0;
        int currentWidth = 0;
        int maxWidth = 0;
        std::string currentField = "";
};