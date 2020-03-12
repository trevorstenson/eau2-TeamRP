#pragma once

#include <fstream>
#include <stdio.h>
#include <iostream>
#include <vector>
#include "type.h"
#include "parse_state.h"
#include "dataframe.h"
#include "util.h"

using namespace std;

/**
 * Object representation of a .sor file
 * Parsing of .sor is done in the constructor of the class
 */
class SorAdapter {
    public:
        DataFrame* df_;
        Schema* schema_;

        /**
         * Constructor for SorAdapter class
         */
        SorAdapter(
            unsigned int from,
            unsigned int length,
            char* filename
        ) {
            string file = string(filename);
            inferSchema(file, from, length);
            buildDataFrame(file, from, length);
        }

        //It will need to be implemented in future assignments. 
        void inferSchema(string filename, unsigned int from, unsigned int length) {
            // Used to keep track of the current state of parsing
            ParseState parseState = ParseState();
            // Used to collect all the data within a row. Once a newline is hit, 
            // the staging vector will be pushed to all of the columns. This is
            // done to prevent partial line processing due to a length argument.
            vector<char> typeVector;

            //create the filestream and skip to the proper location in the file
            fstream fin(filename, fstream::in);
            if (from != 0) {
                skipTo(fin, from);
            }

            // go character by character until the given length is reached
            while (fin >> noskipws >> parseState.ch && parseState.bytesRead < length && parseState.lineCount < 500) {
                parseState.bytesRead++;
                int ascii = (int)parseState.ch;
                switch (ascii) {
                    // New line Character
                    case 10:
                        handleNewLine2(&parseState, &typeVector);
                        break;
                    // < Character
                    case 60:
                        handleOpenTag2(&parseState);
                        break;
                    // > Character
                    case 62:
                        handleCloseTag2(&parseState, &typeVector);
                        break;
                    // " Character
                    case 34:
                        handleQuote2(&parseState);
                        break;
                    // Space character
                    case 32:
                        if (parseState.inQuotes) {
                            parseState.currentField = parseState.currentField + parseState.ch;
                        }
                        break;
                    default:
                        if (parseState.inField) {
                            parseState.currentField = parseState.currentField + parseState.ch;
                        }
                        break;
                }
            }
            char* types = new char[typeVector.size() + 1];
            for (int i = 0; i < typeVector.size(); i++) {
                types[i] = typeVector[i];
            }
            types[typeVector.size()] = '\0';
            schema_ = new Schema(types);
            delete[] types;
        }

        /**
         * Parses the .sor file at filename, loading data into columns vector
         * @param filename .sor filename to be processed
         * @param from the number of bytes to skip forward
         * @param length Number of bytes to be read
         */
        void buildDataFrame(string filename, unsigned int from, unsigned int length) {
            //Initiazlie DataFrame with the inferred schema
            df_ = new DataFrame(*schema_);
            // Used to keep track of the current state of parsing
            ParseState parseState = ParseState();
            // Used to collect all the data within a row. Once a newline is hit, 
            // the staging vector will be pushed to all of the columns. This is
            // done to prevent partial line processing due to a length argument.
            vector<string> stagingVector;

            //create the filestream and skip to the proper location in the file
            fstream fin(filename, fstream::in);
            if (from != 0) {
                skipTo(fin, from);
            }

            // go character by character until the given length is reached
            while (fin >> noskipws >> parseState.ch && parseState.bytesRead < length) {
                parseState.bytesRead++;
                int ascii = (int)parseState.ch;
                switch (ascii) {
                    // New line Character
                    case 10:
                        handleNewLine(&parseState, &stagingVector);
                        break;
                    // < Character
                    case 60:
                        handleOpenTag(&parseState);
                        break;
                    // > Character
                    case 62:
                        handleCloseTag(&parseState, &stagingVector);
                        break;
                    // " Character
                    case 34:
                        handleQuote(&parseState);
                        break;
                    // Space character
                    case 32:
                        if (parseState.inQuotes) {
                            parseState.currentField = parseState.currentField + parseState.ch;
                        }
                        break;
                    default:
                        if (parseState.inField) {
                            parseState.currentField = parseState.currentField + parseState.ch;
                        }
                        break;
                }
            }
            //If end of file was reached with no newline, add the staging vector
            if (parseState.bytesRead < length) {
                writeData(stagingVector, parseState.lineCount);
            }
        }

        /**
         * Skips the fstream ahead from the start to "from" bytes in,
         * and then skips to the end of the line to eliminate partial start line.
         * @param fin The file stream that will be read in
         * @param from The number of bytes to skip forward
         */
        void skipTo(fstream &fin, unsigned int from) {
            unsigned int bytesRead = 0;
            char ch;
            while (bytesRead < from && fin >> noskipws >> ch) {
                bytesRead++;
            }
            // skips to the end of the line to eliminate partial start line.
            while ((int)ch != 10 && fin >> noskipws >> ch) { }
        }

        /**
         * Updates the given ParseState and typeVector when a new line is encountered
         * @param parseState The given ParseState configuration
         * @param typeVector The current typeVector
         */
        void handleNewLine2(ParseState* parseState, vector<char>*  typeVector) {
            if (!parseState->inQuotes) {
                parseState->lineCount++;
                parseState->inField = false;
                if (parseState->currentWidth > parseState->maxWidth) {
                    // update the max width of the parse state
                    parseState->maxWidth = parseState->currentWidth;
                }
                parseState->currentWidth = 0;
            } else {
                parseState->currentField = parseState->currentField + parseState->ch;
            }
        }

        /**
         * Updates the given ParseState when a `<` is encountered
         * @param parseState The given ParseState configuration
         */
        void handleOpenTag2(ParseState* parseState) {
            if (parseState->inQuotes || parseState->inField) {
                parseState->currentField = parseState->currentField + parseState->ch;
            } else if (!parseState->inField) {
                // set the inField flag to true to indicate we are at the beginning of a new field
                parseState->inField = true;
            }
        }

        /**
         * Updates the given ParseState and stagingVector when a `>` is encountered
         * @param parseState The given ParseState configuration
         * @param stagingVector The current stagingVector
         */
        void handleCloseTag2(ParseState* parseState, vector<char>* typeVector) {
            if (!parseState->inQuotes) {
                if (parseState->inField) {
                    if (typeVector->size() <= parseState->currentWidth) {
                        typeVector->push_back(mapToChar(getFieldType(parseState->currentField)));
                    } else {
                        typeVector->data()[parseState->currentWidth] = updateType(mapToType(typeVector->at(parseState->currentWidth)), getFieldType(parseState->currentField));
                    }
                    parseState->currentWidth++;
                    parseState->inField = false;
                    parseState->currentField = "";
                }
            } else {
                parseState->currentField = parseState->currentField + parseState->ch;
            }
        }

        /**
         * Updates the type of this Column if the incomingType is less restrictive than the currentType
         * I.e. STRING replaces FLOAT replaces INT replaces BOOL
         * @param incomingType The type that may replace this column's current Type
         */
        char updateType(Type incomingType, Type newType) {
            // check if the old column Type should be updated
            if (shouldChangeType(newType, incomingType)) {
                return mapToChar(incomingType);
            } else {
                return mapToChar(newType);
            }
        }

        /**
         * Updates the given ParseState when a quote is encountered
         * @param parseState The given ParseState configuration
         */
        void handleQuote2(ParseState* parseState) {
            if (parseState->inField) {
                parseState->inQuotes = !parseState->inQuotes;
                parseState->currentField = parseState->currentField + parseState->ch;
            }
        }


        /**
         * Updates the given ParseState and stagingVector when a new line is encountered
         * @param parseState The given ParseState configuration
         * @param stagingVector The current stagingVector
         */
        void handleNewLine(ParseState* parseState, vector<string>* stagingVector) {
            if (!parseState->inQuotes) {
                // add the staging vector to columns
                writeData(*stagingVector, parseState->lineCount);
                // clear the stagingVector for the next line
                stagingVector->clear();
                parseState->lineCount++;
                parseState->inField = false;
                if (parseState->currentWidth > parseState->maxWidth) {
                    // update the max width of the parse state
                    parseState->maxWidth = parseState->currentWidth;
                }
                parseState->currentWidth = 0;
            } else {
                parseState->currentField = parseState->currentField + parseState->ch;
            }
        }

        /**
         * Updates the given ParseState when<` is encountered
         * @param parseState The given ParseState configuration
         */
        void handleOpenTag(ParseState* parseState) {
            if (parseState->inQuotes || parseState->inField) {
                parseState->currentField = parseState->currentField + parseState->ch;
            } else if (!parseState->inField) {
                // set the inField flag to true to indicate we are at the beginning of a new field
                parseState->inField = true;
            }
        }

        /**
         * Updates the given ParseState and stagingVector when a `>` is encountered
         * @param parseState The given ParseState configuration
         * @param stagingVector The current stagingVector
         */
        void handleCloseTag(ParseState* parseState, vector<string>* stagingVector) {
            if (!parseState->inQuotes) {
                if (parseState->inField) {
                    parseState->currentWidth++;
                    parseState->inField = false;
                    trim(parseState->currentField);
                    stagingVector->push_back(parseState->currentField);
                    parseState->currentField = "";
                }
            } else {
                parseState->currentField = parseState->currentField + parseState->ch;
            }
        }

        /**
         * Updates the given ParseState when a quote is encountered
         * @param parseState The given ParseState configuration
         */
        void handleQuote(ParseState* parseState) {
            if (parseState->inField) {
                parseState->inQuotes = !parseState->inQuotes;
                parseState->currentField = parseState->currentField + parseState->ch;
            }
        }

        /**
         * Adds the staging vector to the columns
         * @param stagingVector vector of strings representing a row of data to be added
         * @param row Row number, starting at 1
         */  
        void writeData(vector<string> stagingVector, int row) {
            for (int i = 0; i < stagingVector.size(); i++) {
                string currentField = stagingVector[i];
                //add the data contained within the field to columns
                if (currentField != "") {
                    switch(schema_->type(i)) {
                        case 'B':
                            df_->set(i, row, parseBool(currentField));
                            break;
                        case 'F':
                            df_->set(i, row, parseFloat(currentField));
                            break;
                        case 'S':
                            df_->set(i, row, parseString(currentField));
                            break;
                        case 'I':
                            df_->set(i, row, parseInt(currentField));
                            break;
                        default:
                            assert("Unrecognized type" && false);
                    }
                }
            }
        }

        DataFrame* get_df() {
            return df_;
        }

        ~SorAdapter() {
            //Delete schema and df
            delete schema_;
            delete df_;
        }
};