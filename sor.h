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

        //NOTE: This was implemented hard-coded to prototype reading in with our SorAdapter.
        //It will need to be implemented in future assignments. 
        void inferSchema(string filename, unsigned int from, unsigned int length) {
            schema_ = new Schema("BFSIBFSIBF");
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
         * Updates the given ParseState when a `<` is encountered
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

        /**
         * Prints the Sor. Useful for debugging.
         */
        // void printTransposed() {
        //     cout << "\nTranspose:\n";
        //     for (Column* c: columns) {
        //         c->print(10);
        //         cout << "\n";
        //     }
        // }

        /**
         * Prints the column type of the column at the specified index
         * @param column the index of the column (starting at 0)
         */
        // void printColumnType(unsigned int column) {
        //     if (column <= columns.size() - 1) {
        //         Type type = schema_->type(column);
        //         cout << schema_->type(column) << endl;
        //     } else {
        //         assert("Invalid column. Program terminated." && false);
        //     }
        // }

        /**
         * Prints the string for the value at the given column and row
         * @param col Column index (starting at 0)
         * @param offset Row index (starting at 0)
         */
        // void printValue(unsigned int col, unsigned int offset) {
        //     string value = getValueAt(col, offset);
        //     if (columns[col]->getType() == STRING) {
        //         value = "\"" + value + "\"";
        //     }
        //     cout << value << endl;
        // }
        
        /**
         * Gets the string for the value at the given column and row
         * @param column Column index (starting at 0)
         * @param row Row index (starting at 0)
         * @return The value's string representation
         */
        // string getValueAt(unsigned int column, unsigned int row) {
        //     if (column >= columns.size()) {
        //         assert("Column index out of bounds exception." && false);
        //     }
        //     string* str = columns[column]->getValue(row);
        //     if (str == nullptr) {
        //         return "";
        //     } else {
        //         return *str;
        //     }
        // }

        /**
         * Prints 1 if the value of the given column and row is missing
         * @param col the index of the column (starting at 0)
         * @param row the index of the row (starting at 0)
         */
        // void printIsMissing(unsigned int col, unsigned int row) {
        //     bool missing = 0;
        //     if (col <= columns.size() - 1) {
        //         if (row > getMaxColumnHeight() - 1) {
        //             assert("Index out of bounds. Program terminated." && false);
        //         } else if (row <= columns[col]->values.size() - 1) {
        //             if (columns[col]->getValue(row) == nullptr) {
        //                 missing = 1;
        //             }
        //         } else {
        //             missing = 1;
        //         }
        //     } else {
        //         assert("Index out of bounds. Program terminated." && false);
        //     }
        //     cout << missing << endl;
        // } 

        // unsigned int getMaxColumnHeight() {
        //     unsigned int max = 0;
        //     for(int i = 0; i < columns.size() - 1; i++) {
        //         int height = columns[i]->length();
        //         if (height > max) max = height;
        //     }
        //     return max;
        // }

        DataFrame* get_df() {
            return df_;
        }

        ~SorAdapter() {
            //Delete schema and df
            delete schema_;
            delete df_;
        }
};