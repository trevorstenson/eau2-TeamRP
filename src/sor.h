#pragma once

#include <fstream>
#include <stdio.h>
#include <iostream>
#include <vector>
#include "type.h"
#include "dataframe.h"
#include "util.h"

using namespace std;


/**
 * Keeps track of information regarding the current
 * state of parsing within the .sor file
 */
struct ParseState {
    public: 
        char ch;
        bool in_field = false;
        bool in_quotes = false;
        unsigned int bytes_read = 0;
        int line_count = 0;
        int current_width = 0;
        int max_width = 0;
        std::string current_field = "";
};

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
            infer_schema(file, from, length);
            build_DataFrame(file, from, length);
        }

        //It will need to be implemented in future assignments. 
        void infer_schema(string filename, unsigned int from, unsigned int length) {
            // Used to keep track of the current state of parsing
            ParseState parse_state = ParseState();
            // Used to collect all the data within a row. Once a newline is hit, 
            // the staging vector will be pushed to all of the columns. This is
            // done to prevent partial line processing due to a length argument.
            vector<char> type_vector;

            //create the filestream and skip to the proper location in the file
            fstream fin(filename, fstream::in);
            if (from != 0) {
                skip_to(fin, from);
            }

            // go character by character until the given length is reached
            while (fin >> noskipws >> parse_state.ch && parse_state.bytes_read < length && parse_state.line_count < 500) {
                parse_state.bytes_read++;
                int ascii = (int)parse_state.ch;
                switch (ascii) {
                    // New line Character
                    case 10:
                        handle_new_line2(&parse_state, &type_vector);
                        break;
                    // < Character
                    case 60:
                        handle_open_tag2(&parse_state);
                        break;
                    // > Character
                    case 62:
                        handle_close_tag2(&parse_state, &type_vector);
                        break;
                    // " Character
                    case 34:
                        handle_quote2(&parse_state);
                        break;
                    // Space character
                    case 32:
                        if (parse_state.in_quotes) {
                            parse_state.current_field = parse_state.current_field + parse_state.ch;
                        }
                        break;
                    default:
                        if (parse_state.in_field) {
                            parse_state.current_field = parse_state.current_field + parse_state.ch;
                        }
                        break;
                }
            }
            char* types = new char[type_vector.size() + 1];
            for (int i = 0; i < type_vector.size(); i++) {
                types[i] = type_vector[i];
            }
            types[type_vector.size()] = '\0';
            schema_ = new Schema(types);
            delete[] types;
        }

        /**
         * Parses the .sor file at filename, loading data into columns vector
         * @param filename .sor filename to be processed
         * @param from the number of bytes to skip forward
         * @param length Number of bytes to be read
         */
        void build_DataFrame(string filename, unsigned int from, unsigned int length) {
            //Initiazlie DataFrame with the inferred schema
            df_ = new DataFrame(*schema_);
            // Used to keep track of the current state of parsing
            ParseState parse_state = ParseState();
            // Used to collect all the data within a row. Once a newline is hit, 
            // the staging vector will be pushed to all of the columns. This is
            // done to prevent partial line processing due to a length argument.
            vector<string> staging_vector;

            //create the filestream and skip to the proper location in the file
            fstream fin(filename, fstream::in);
            if (from != 0) {
                skip_to(fin, from);
            }

            // go character by character until the given length is reached
            while (fin >> noskipws >> parse_state.ch && parse_state.bytes_read < length) {
                parse_state.bytes_read++;
                int ascii = (int)parse_state.ch;
                switch (ascii) {
                    // New line Character
                    case 10:
                        handle_new_line(&parse_state, &staging_vector);
                        break;
                    // < Character
                    case 60:
                        handle_open_tag(&parse_state);
                        break;
                    // > Character
                    case 62:
                        handle_close_tag(&parse_state, &staging_vector);
                        break;
                    // " Character
                    case 34:
                        handle_quote(&parse_state);
                        break;
                    // Space character
                    case 32:
                        if (parse_state.in_quotes) {
                            parse_state.current_field = parse_state.current_field + parse_state.ch;
                        }
                        break;
                    default:
                        if (parse_state.in_field) {
                            parse_state.current_field = parse_state.current_field + parse_state.ch;
                        }
                        break;
                }
            }
            //If end of file was reached with no newline, add the staging vector
            if (parse_state.bytes_read < length) {
                write_data(staging_vector, parse_state.line_count);
            }
        }

        /**
         * Skips the fstream ahead from the start to "from" bytes in,
         * and then skips to the end of the line to eliminate partial start line.
         * @param fin The file stream that will be read in
         * @param from The number of bytes to skip forward
         */
        void skip_to(fstream &fin, unsigned int from) {
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
        void handle_new_line2(ParseState* parseState, vector<char>*  typeVector) {
            if (!parseState->in_quotes) {
                parseState->line_count++;
                parseState->in_field = false;
                if (parseState->current_width > parseState->max_width) {
                    // update the max width of the parse state
                    parseState->max_width = parseState->current_width;
                }
                parseState->current_width = 0;
            } else {
                parseState->current_field = parseState->current_field + parseState->ch;
            }
        }

        /**
         * Updates the given ParseState when a `<` is encountered
         * @param parseState The given ParseState configuration
         */
        void handle_open_tag2(ParseState* parseState) {
            if (parseState->in_quotes || parseState->in_field) {
                parseState->current_field = parseState->current_field + parseState->ch;
            } else if (!parseState->in_field) {
                // set the inField flag to true to indicate we are at the beginning of a new field
                parseState->in_field = true;
            }
        }

        /**
         * Updates the given ParseState and stagingVector when a `>` is encountered
         * @param parseState The given ParseState configuration
         * @param stagingVector The current stagingVector
         */
        void handle_close_tag2(ParseState* parseState, vector<char>* typeVector) {
            if (!parseState->in_quotes) {
                if (parseState->in_field) {
                    if (typeVector->size() <= parseState->current_width) {
                        typeVector->push_back(map_to_char(get_field_type(parseState->current_field)));
                    } else {
                        typeVector->data()[parseState->current_width] = update_type(map_to_type(typeVector->at(parseState->current_width)), get_field_type(parseState->current_field));
                    }
                    parseState->current_width++;
                    parseState->in_field = false;
                    parseState->current_field = "";
                }
            } else {
                parseState->current_field = parseState->current_field + parseState->ch;
            }
        }

        /**
         * Updates the type of this Column if the incomingType is less restrictive than the currentType
         * I.e. STRING replaces FLOAT replaces INT replaces BOOL
         * @param incomingType The type that may replace this column's current Type
         */
        char update_type(Type incomingType, Type newType) {
            // check if the old column Type should be updated
            if (should_change_type(newType, incomingType)) {
                return map_to_char(incomingType);
            } else {
                return map_to_char(newType);
            }
        }

        /**
         * Updates the given ParseState when a quote is encountered
         * @param parseState The given ParseState configuration
         */
        void handle_quote2(ParseState* parseState) {
            if (parseState->in_field) {
                parseState->in_quotes = !parseState->in_quotes;
                parseState->current_field = parseState->current_field + parseState->ch;
            }
        }


        /**
         * Updates the given ParseState and stagingVector when a new line is encountered
         * @param parseState The given ParseState configuration
         * @param stagingVector The current stagingVector
         */
        void handle_new_line(ParseState* parse_state, vector<string>* stagingVector) {
            if (!parse_state->in_quotes) {
                // add the staging vector to columns
                write_data(*stagingVector, parse_state->line_count);
                // clear the stagingVector for the next line
                stagingVector->clear();
                parse_state->line_count++;
                parse_state->in_field = false;
                if (parse_state->current_width > parse_state->max_width) {
                    // update the max width of the parse state
                    parse_state->max_width = parse_state->current_width;
                }
                parse_state->current_width = 0;
            } else {
                parse_state->current_field = parse_state->current_field + parse_state->ch;
            }
        }

        /**
         * Updates the given ParseState when<` is encountered
         * @param parse_state The given ParseState configuration
         */
        void handle_open_tag(ParseState* parse_state) {
            if (parse_state->in_quotes || parse_state->in_field) {
                parse_state->current_field = parse_state->current_field + parse_state->ch;
            } else if (!parse_state->in_field) {
                // set the inField flag to true to indicate we are at the beginning of a new field
                parse_state->in_field = true;
            }
        }

        /**
         * Updates the given ParseState and stagingVector when a `>` is encountered
         * @param parse_state The given ParseState configuration
         * @param staging_vector The current stagingVector
         */
        void handle_close_tag(ParseState* parse_state, vector<string>* staging_vector) {
            if (!parse_state->in_quotes) {
                if (parse_state->in_field) {
                    parse_state->current_width++;
                    parse_state->in_field = false;
                    trim(parse_state->current_field);
                    staging_vector->push_back(parse_state->current_field);
                    parse_state->current_field = "";
                }
            } else {
                parse_state->current_field = parse_state->current_field + parse_state->ch;
            }
        }

        /**
         * Updates the given ParseState when a quote is encountered
         * @param parse_state The given ParseState configuration
         */
        void handle_quote(ParseState* parse_state) {
            if (parse_state->in_field) {
                parse_state->in_quotes = !parse_state->in_quotes;
                parse_state->current_field = parse_state->current_field + parse_state->ch;
            }
        }

        /**
         * Adds the staging vector to the columns
         * @param staging_vector vector of strings representing a row of data to be added
         * @param row Row number, starting at 1
         */  
        void write_data(vector<string> staging_vector, int row) {
            for (int i = 0; i < staging_vector.size(); i++) {
                string current_field = staging_vector[i];
                //add the data contained within the field to columns
                if (current_field != "") {
                    switch(schema_->type(i)) {
                        case 'B':
                            df_->set(i, row, parse_bool(current_field));
                            break;
                        case 'F':
                            df_->set(i, row, parse_float(current_field));
                            break;
                        case 'S':
                            df_->set(i, row, parse_string(current_field));
                            break;
                        case 'I':
                            df_->set(i, row, parse_int(current_field));
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