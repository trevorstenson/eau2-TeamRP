//CwC

/**
 * Convenience wrapper around stoi, returns an integer from a string
 * @param s The string to be parsed
 * @return the parsed integer
 * 
 */
int parseInt(string value) {
    if (value == "") {
        return NULL;
    } else {
        return stoi(value);
    }
}

/**
 * Convenience wrapper around stof, returns a float from a string
 * @param s The string to be parsed
 * @return parsed float
 */
float parseFloat(string value) {
    if (value == "") {
        return NULL;
    } else {
        return stof(value);
    }
}

/**
 * Returns bool from given string
 * @param s The string to be parsed
 * @return bool from string
 */
bool parseBool(string value) {
    if (value == "") {
        return NULL;
    } else if (value == "1") {
        return 1;
    } else if (value == "0") {
        return 0;
    } else {
        string message = "Unable to parse boolean from string: ";
        message.append(value);
        throw message;
    }
}

/**
 * Returns String from given string
 * @param s The string to be parsed
 * @return bool from string
 */
String* parseString(string value) {
    if (value == "") {
        return nullptr;
    } else {
        return new String(value.c_str());
    }
}