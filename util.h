//CwC

#include <regex>
#include "type.h"
#include <string>

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

/**
 * Returns if the string is an integer
 * @param s The string to be evaluated
 * @return if s is an integer
 */
bool isInt(string value) {
    if (value.find("\"")  != string::npos) {
        return false;
    } else {
        string intPattern(R"(^(?: *)(?:\+|\-|)(\d+))");
        regex intRegex(intPattern);
        return regex_match(value, intRegex);
    }
}

/**
 * Is the string is a float
 * @param s The string to be evaluated
 * @return bool if the string is a float
 */
bool isFloat(string value) {
    if (value.find("\"")  != string::npos) {
        return false;
    } else {
        string floatPattern(R"(^(?: *)(?:\+|\-|)(\d+).(\d+))");
        regex floatRegex(floatPattern);
        return regex_match(value, floatRegex);
    }
}

/**
 * Returns if the string is a 0 or 1, defined to be a bool by the assignment 1 spec
 * @param s The string to be evaluated
 * @return bool: if string is 1 or 0
 */
bool isBool(string value) {
    if (value.find("\"")  != string::npos) {
        return false;
    } else {
        return value == "1" || value == "0";
    }
}

/**
 * Returns if a given value is a string
 * @param value The string to be evaluated
 * @return bool from string
 */
bool isString(string value) {
    if (value == "") return true;
    //regex used when in quotes
    regex quotedRegex("^(\")(.*)(\")");
    //regex used when no quotes present
    regex nonQuotedRegex("^ *[^ ]+ *$");
    if (value.find("\"") != string::npos) {
      return regex_match(value, quotedRegex);
    }
    return regex_match(value, nonQuotedRegex);
}

/**
 * Determines the most restrictive type that can be applied to a string
 *  @param fieldValue The string to be evaluated
 *  @return The most restrictive type fieldValue can represent
 */ 
Type getFieldType(string fieldValue) {
    if (isBool(fieldValue)) return BOOL;
    if (isInt(fieldValue)) return INT;
    if (isFloat(fieldValue)) return FLOAT;
    if (isString(fieldValue)) return STRING;
    return BOOL;
}

/**
 * Relating to the previous function, updateColumnType. True if newType is less restrictive. False otherwise
 * @param oldType The original type of a columnn
 * @param newType The new type coming in
 * @return True if the newType should override/replace the old type, false otherwise
 */
bool shouldChangeType(Type oldType, Type newType) {
    return newType > oldType;
}

/**
 * Maps character to Types
 * @param type The character representation on the type
 * @return the type
 */
Type mapToType(char type) {
        cout << type << flush;
    switch (type) {
        case 'B':
            return BOOL;
            break;
        case 'S':
            return STRING;
            break;
        case 'F':
            return FLOAT;
            break;
        case 'I':
            return INT;
            break;
        default:
            assert("\nUnrecognized type" && false);
            break;
    }
}

/**
 * Maps Types to character
 * @param type The Type 
 * @return the character representation of the type
 */
char mapToChar(Type type) {
    switch (type) {
        case BOOL:
            return 'B';
        case STRING:
            return 'S';
        case FLOAT:
            return 'F';
        case INT:
            return 'I';
        default:
            assert("Unreciognized type" && false);
            break;
    }
}