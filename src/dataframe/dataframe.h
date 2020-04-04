//lang: CwC
#pragma once

#include "column.h"
#include "row.h"
#include "rower.h"
#include "schema.h"
#include "../serial/serial.h"
#include "../serial/array.h"
#include "../object.h"
#include <thread>
#include <mutex>
#include <functional>
#include <string>

//KVStore
#include "../map.h"
#include "../store/key.h"
#include "../store/value.h"
#include "../store/networkconfig.h"
#include <atomic>
#include <stdio.h>  
#include <stdlib.h>  
#include <errno.h>  
#include <unistd.h>
#include <arpa/inet.h>
#include <sys/types.h>  
#include <sys/socket.h>  
#include <netinet/in.h>  
#include <sys/time.h>
#include <sys/ioctl.h>

#define MAX_THREADS 8
#define MIN_LINES 1000

#define BUFF_SIZE 1024
#define TEMP_CLIENTS_MAX 30

//Forward declaration for KVStore
class DataFrame;

//Forward declaration so `put` can be used in DataFrame
class KVStore : public Object {
public:
    KVMap kv_map_;
    size_t idx_;
    NetworkConfig nconfig_;
    DataFrame* waitAndGetValue;
    KVStore();
    ~KVStore();
    bool containsKey(Key *k);
    Value *put(Key &k, Value *v);
    Value *put(Key &k, unsigned char *data, size_t length);
    DataFrame *get(Key &k);
    DataFrame *waitAndGet(Key &k);
    void setIndex(size_t idx);
    void configure(const char* ip, int port, const char* serverIp, int serverPort);
    void configure(const char* ip, const char* serverIp, int serverPort);
    void sendToServer(unsigned char* msg);
    void registerWithServer();
    void initializePeerToPeer();
    void listenToNeighbors();
    unsigned char* readIncomingNodeMsg(int fd);
    void handleDisconnect(int fd);
    void handleNodeMsg(int fd, unsigned char* msg);
    void sendToNeighbor(int fd, unsigned char* msg);
    void handleStatus(int fd, unsigned char* msg);
    void handlePut(int fd, unsigned char* msg);
    void handleGet(int fd, unsigned char* msg);
    void handleResult(int fd, unsigned char* msg);
    void listenToServer();
    void handleIncoming(unsigned char* data);
    void shutdown();
    void closeNodeConnections();
    void closeServerConnection();
    void updateConnections(unsigned char* data);
    void createNeighborConnections();
    void greetAllNeighbors();
    void handleAck(Ack* ack);
    void handleNack(Nack* nack);
    unsigned char* getRegistrationMessage();
};

/****************************************************************************
 * DataFrame::
 *
 * A DataFrame is table composed of columns of equal length. Each column
 * holds values of the same type (I, S, B, F). A dataframe has a schema that
 * describes it.
 * Authors:
 * Canon Sawrey sawrey.c@husky.neu.edu
 * Trevor Stenson stenson.t@husky.neu.edu
 */
class DataFrame : public Object, public Serializable {
public:
    Schema *schema;
    size_t col_cap;
    Column **columns;

    /** Create a data frame with the same columns as the given df but with no rows or rownmaes */
    DataFrame(DataFrame &df) {
        Schema *newSchema = new Schema(df.schema->types);
        schema = newSchema;
        col_cap = df.col_cap;
        columns = new Column *[col_cap];
        for (int i = 0; i < schema->width(); i++) {
            switch (schema->type(i)) {
            case 'B':
                columns[i] = new BoolColumn();
                break;
            case 'I':
                columns[i] = new IntColumn();
                break;
            case 'S':
                columns[i] = new StringColumn();
                break;
            case 'D':
                columns[i] = new DoubleColumn();
                break;
            default:
                assert("Type other than B, I, F, or S found." && false);
            }
        }
    }

    /** Create a data frame from a schema and columns. All columns are created
    * empty. */
    DataFrame(Schema &schema_) {
        schema = &schema_;
        col_cap = schema->col_cap;
        columns = new Column *[col_cap];
        for (int i = 0; i < schema->width(); i++)
        {
            switch (schema->type(i))
            {
            case 'B':
                columns[i] = new BoolColumn();
                break;
            case 'I':
                columns[i] = new IntColumn();
                break;
            case 'S':
                columns[i] = new StringColumn();
                break;
            case 'D':
                columns[i] = new DoubleColumn();
                break;
            default:
                assert("Type other than B, I, F, or S found." && false);
            }
        }
    }

    DataFrame(unsigned char *serial) {
        deserialize(serial);
    }

    /** Returns the dataframe's schema. Modifying the schema after a dataframe
    * has been created in undefined. */
    Schema &get_schema() {
        //Schema* temp = new Schema(schema);
        //return *temp;
        return *schema;
    }

    /** Adds a column this dataframe, updates the schema, the new column
    * is external, and appears as the last column of the dataframe, the
    * name is optional and external. A nullptr colum is undefined. */
    void add_column(Column *col, String *name) {
        if (col == nullptr) {
            assert("Cannot have nullptr col." && false);
        }
        //Copy column
        Column *addCol;
        switch (col->type) {
        case 'B':
            addCol = col->as_bool()->clone();
            break;
        case 'D':
            addCol = col->as_double()->clone();
            break;
        case 'I':
            addCol = col->as_int()->clone();
            break;
        case 'S':
            addCol = col->as_string()->clone();
            break;
        default:
            break;
        }
        ensureColumnCapacity();
        columns[schema->width()] = addCol;
        schema->new_length(addCol->size());
        schema->add_column(col->get_type(), name);
    }

    /** Ensures the current col capacity is enough to
     * accomodate an addition */
    void ensureColumnCapacity() {
        if (schema->width() == col_cap)
        {
            growColumns();
        }
    }

    /** Increase columm capacity and copies over old values */
    void growColumns() {
        size_t previous = col_cap;
        if (col_cap == 0) {
            col_cap++;
        } else {
            col_cap *= 2;
        }
        Column **newValues = new Column *[col_cap];
        for (size_t i = 0; i < previous; i++) {
            newValues[i] = columns[i];
        }
        delete[] columns;
        columns = newValues;
    }

    /** Return the value at the given column and row. Accessing rows or
   *  columns out of bounds, or request the wrong type is undefined.*/
    int get_int(size_t col, size_t row) {
        checkIndices(col, row, 'I');
        return columns[col]->as_int()->get(row);
    }
    bool get_bool(size_t col, size_t row) {
        checkIndices(col, row, 'B');
        return columns[col]->as_bool()->get(row);
    }
    double get_double(size_t col, size_t row) {
        checkIndices(col, row, 'D');
        return columns[col]->as_double()->get(row);
    }
    String *get_string(size_t col, size_t row) {
        checkIndices(col, row, 'S');
        return columns[col]->as_string()->get(row);
    }

    /** Set the value at the given column and row to the given value.
    * If the column is not  of the right type or the indices are out of
    * bound, the result is undefined. */
    void set(size_t col, size_t row, int val) {
        checkIndices(col, row, 'I');
        schema->new_length(row);
        IntColumn *column = columns[col]->as_int();
        if (!column) {
            assert("Unable to cast as IntColumn." && false);
        } else {
            column->set(row, val);
        }
    }
    void set(size_t col, size_t row, bool val) {
        checkIndices(col, row, 'B');
        schema->new_length(row);
        BoolColumn *column = columns[col]->as_bool();
        if (!column) {
            assert("Unable to cast as BoolColumn." && false);
        } else {
            column->set(row, val);
        }
    }
    void set(size_t col, size_t row, double val) {
        checkIndices(col, row, 'D');
        schema->new_length(row);
        DoubleColumn *column = columns[col]->as_double();
        if (!column) {
            assert("Unable to cast as DoubleColumn." && false);
        } else {
            column->set(row, val);
        }
    }
    void set(size_t col, size_t row, String *val) {
        checkIndices(col, row, 'S');
        schema->new_length(row);
        StringColumn *column = columns[col]->as_string();
        if (!column) {
            assert("Unable to cast as StringColumn." && false);
        } else {
            column->set(row, val);
        }
    }

    /** Ensures the value requested matches the schema and is in the DF's bounds */
    void checkIndices(size_t col, size_t row, char type) {
        if (col >= schema->n_col) {
            assert("Index out of bounds." && false);
        } else if (schema->type(col) != type) {
            assert("Type mismatch." && false);
        }
    }

    /** Set the fields of the given row object with values from the columns at
    * the given offset.  If the row is not form the same schema as the
    * dataframe, results are undefined.
    */
    void fill_row(size_t idx, Row &row) {
        if (matchingSchema(row)) {
            row.set_idx(idx);
            for (size_t i = 0; i < ncols(); i++) {
                char type = columns[i]->get_type();
                switch (type) {
                case 'I':
                {
                    IntColumn *intCol = columns[i]->as_int();
                    row.set(i, intCol->get(idx));
                    break;
                }
                case 'B':
                {
                    BoolColumn *boolCol = columns[i]->as_bool();
                    row.set(i, boolCol->get(idx));
                    break;
                }
                case 'D':
                {
                    DoubleColumn *fCol = columns[i]->as_double();
                    row.set(i, fCol->get(idx));
                    break;
                }
                case 'S':
                {
                    StringColumn *sCol = columns[i]->as_string();
                    row.set(i, sCol->get(idx));
                    break;
                }
                default:
                    assert("Invalid operation." && false);
                }
            }
        }
    }

    /** Add a row at the end of this dataframe. The row is expected to have
   *  the right schema and be filled with values, otherwise undedined.  */
    void add_row(Row &row)
    {
        if (!matchingSchema(row)) {
            assert("Incorrect row schema." && false);
        } else {
            set_row(nrows(), row);
        }
    }

    void set_row(size_t idx, Row &row)
    {
        if (!matchingSchema(row)) {
            assert("Incorrect row schema." && false);
        } else {
            //Update schema to know the number of rows - use idx + 1 to get length
            schema->new_length(idx);
            //Load data into columns
            for (size_t i = 0; i < row.width(); i++) {
                char type = schema->type(i);
                switch (type)
                {
                case 'I':
                {
                    columns[i]->as_int()->set(idx, row.get_int(i));
                    break;
                }
                case 'B':
                {
                    columns[i]->as_bool()->set(idx, row.get_bool(i));
                    break;
                }
                case 'D':
                {
                    columns[i]->as_double()->set(idx, row.get_double(i));
                    break;
                }
                case 'S':
                {
                    columns[i]->as_string()->set(idx, row.get_string(i));
                    break;
                }
                default:
                    assert("Invalid type. Program terminated." && false);
                }
            }
        }
    }

    /** Determines if the given row matches the schema of 
     * this Schema* */
    bool matchingSchema(Row &row) {
        return strcmp(row.get_schema()->types, schema->types) == 0;
    }

    /** The number of rows in the dataframe. */
    size_t nrows() {
        return schema->length();
    }

    /** The number of columns in the dataframe.*/
    size_t ncols() {
        return schema->width();
    }

    /** Visit rows in order */
    void map(Rower &r) {
        for (size_t i = 0; i < nrows(); i++) {
            Row newRow(get_schema());
            fill_row(i, newRow);
            r.accept(newRow);
        }
    }

    //this method kept throwing errors when attempting to use for the first and last halves.
    void pmapRange(size_t start, size_t end, Rower *r) {
        for (size_t i = start; i < end; i++) {
            Row newRow(get_schema());
            fill_row(i, newRow);
            r->accept(newRow);
        }
    }

    void pmap(Rower &r) {
        size_t threadCount = nrows() / MIN_LINES;
        if (threadCount <= 1) {
            map(r);
        } else {
            threadCount = (threadCount > MAX_THREADS) ? MAX_THREADS : threadCount;
            size_t rowsPerThread = nrows() / threadCount;

            Rower **rowers = new Rower *[threadCount];
            rowers[0] = &r;
            for (int i = 1; i < threadCount; i++) {
                rowers[i] = static_cast<Rower *>(r.clone());
            }

            std::thread *threads[threadCount];
            int startingValue = 0;
            int endValue = 0;
            for (int i = 0; i < threadCount; i++) {
                if (threadCount - i == 1) {
                    endValue = nrows();
                } else {
                    endValue = startingValue + rowsPerThread;
                }
                threads[i] = new std::thread(&DataFrame::pmapRange, this, startingValue, endValue, rowers[i]);
                startingValue = endValue;
            }

            for (int i = 0; i < threadCount; ++i) {
                threads[i]->join();
            }

            for (int i = 1; i < threadCount; i++) {
                rowers[0]->join_delete(rowers[i]);
            }

            for (int i = 0; i < threadCount; i++) {
                delete threads[i];
            }

            delete[] rowers;
        }
    }

    /** Create a new dataframe, constructed from rows for which the given Rower
    * returned true from its accept method. */
    DataFrame *filter(Rower &r) {
        DataFrame *newDataFrame = new DataFrame(*this);
        int nr = nrows();
        for (size_t i = 0; i < nr; i++) {
            Row *newRow = new Row(*newDataFrame->schema);
            fill_row(i, *newRow);
            if (r.accept(*newRow)) {
                newDataFrame->add_row(*newRow);
            }
        }
        return newDataFrame;
    }

    static DataFrame *fromArray(Key *key, KVStore *kv, size_t size, double *array) {
        String *schemaStr = new String("D");
        Schema *newSchema = new Schema(schemaStr->c_str());
        delete schemaStr;
        DataFrame *newDf = new DataFrame(*newSchema);
        for (size_t i = 0; i < size; i++) {
            newDf->set(0, i, array[i]);
        }
        //serialize and add df to kvstore
        unsigned char* serial = newDf->serialize();
        kv->put(*key, serial, extract_size_t(serial, 0));
        return newDf;
    }

    static DataFrame *fromScalar(Key *key, KVStore *kv, double value) {
        String *schemaStr = new String("D");
        Schema *newSchema = new Schema(schemaStr->c_str());
        delete schemaStr;
        DataFrame *newDf = new DataFrame(*newSchema);
        newDf->set(0, 0, value);
        unsigned char* serial = newDf->serialize();
        kv->put(*key, serial, extract_size_t(serial, 0));
        return newDf;
    }

    /** Print the dataframe in SoR format to standard output. */
    void print() {
        for (int i = 0; i < schema->length(); i++) {
            for (int j = 0; j < schema->width(); j++) {
                cout << "<";
                columns[j]->print(i);
                cout << ">";
            }
            if (i < schema->length()) {
                cout << endl;
            }
        }
    }

    bool equals(Object *other) {
        if (other == this)
            return true;
        DataFrame *x = dynamic_cast<DataFrame *>(other);
        if (x == nullptr)
            return false;
        if (!schema->equals(x->schema))
            return false;
        for (int i = 0; i < ncols(); i++) {
            if (!columns[i]->equals(x->columns[i]))
                return false;
        }
        return true;
    }

    /** Convenience printing method for debugging. */
    void debug_print() {
        cout << endl
             << "-----------SCHEMA-----------" << endl;
        schema->debug_print();
        cout << endl
             << "---------END SCHEMA---------" << endl;
        cout << endl
             << "-----------SoR-----------" << endl;
        print();
        cout << endl
             << "---------END SoR---------" << endl;
    }

    unsigned char *serialize() {
        size_t buffer_length = 100;
        unsigned char *serial = new unsigned char[buffer_length];
        unsigned char *schm = schema->serialize();
        size_t index = 8 + 16 + schema->width() + 1;
        copy_unsigned(serial + 8, schm, index - 8);
        for (size_t i = 0; i < schema->width(); i++) {
            if (schema->type(i) == 'S') {
                StringArray *stra = new StringArray(columns[i]);
                unsigned char *temp = stra->serialize();
                size_t length = extract_size_t(temp, 0);

                while (index + length >= buffer_length - 1) {
                    unsigned char *temp = new unsigned char[buffer_length * 2];
                    copy_unsigned(temp, serial, buffer_length);
                    buffer_length *= 2;
                    delete[] serial;
                    serial = temp;
                }

                copy_unsigned(serial + index, temp, length);
                delete[] temp;
                index += length;
            } else {
                DoubleArray *dbl = new DoubleArray(columns[i]);
                unsigned char *temp = dbl->serialize();
                size_t length = extract_size_t(temp, 0);

                while (index + length >= buffer_length - 1) {
                    unsigned char *temp = new unsigned char[buffer_length * 2];
                    copy_unsigned(temp, serial, buffer_length);
                    buffer_length *= 2;
                    delete[] serial;
                    serial = temp;
                }

                copy_unsigned(serial + index, temp, length);
                delete[] temp;
                index += length;
            }
        }
        insert_size_t(index, serial, 0);
        return serial;
    }

    size_t deserialize(unsigned char *serialized) {
        size_t index = 8;
        schema = new Schema(serialized + index);
        col_cap = schema->col_cap;
        columns = new Column *[col_cap];
        index += 16 + schema->width() + 1;

        DoubleArray *dbl;
        StringArray *str;

        for (int i = 0; i < schema->width(); i++) {
            switch (schema->type(i)) {
            case 'B':
                columns[i] = new BoolColumn();
                dbl = new DoubleArray();
                index += dbl->deserialize(serialized + index);
                for (size_t j = 0; j < dbl->len_; j++) {
                    if (dbl->vals_[j] == 0) {
                        set(i, j, false);
                    } else {
                        set(i, j, true);
                    }
                }
                delete dbl;
                break;
            case 'I':
                columns[i] = new IntColumn();
                dbl = new DoubleArray();
                index += dbl->deserialize(serialized + index);
                for (size_t j = 0; j < dbl->len_; j++) {
                    set(i, j, int(dbl->vals_[j]));
                }
                delete dbl;
                break;
            case 'S':
                columns[i] = new StringColumn();
                str = new StringArray();
                index += str->deserialize(serialized + index);
                for (size_t j = 0; j < str->len_; j++) {
                    set(i, j, str->vals_[j]->clone());
                }
                delete str;
                break;
            case 'D':
                columns[i] = new DoubleColumn();
                dbl = new DoubleArray();
                index += dbl->deserialize(serialized + index);
                for (size_t j = 0; j < dbl->len_; j++) {
                    set(i, j, dbl->vals_[j]);
                }
                delete dbl;
                break;
            default:
                assert("Type other than B, I, F, or S found." && false);
            }
        }
        return index;
    };

    ~DataFrame()
    {
        delete[] columns;
    }
};

/** KVStore implementation for the previous forward declaration */
inline KVStore::KVStore() {
    waitAndGetValue = nullptr;
}
inline KVStore::~KVStore() {
    delete waitAndGetValue;
}
inline bool KVStore::containsKey(Key *k) {
    return kv_map_.containsKey(k);
}
inline Value *KVStore::put(Key &k, Value *v) {
    // data is stored in local kvstore
    if (idx_ == k.node_) {
        return kv_map_.put(&k, v);
    } else {
        // Send the data to the correct node TODO change to real network call
        //return mock_network_[k.node_]->put(k, v);
        Put* p = new Put(idx_, k.node_, 1234, &k, v);
        sendToNeighbor(nconfig_.neighborSockets[k.node_], p->serialize());
        return nullptr;
    }
}
inline Value *KVStore::put(Key &k, unsigned char *data, size_t length) {
    return put(k, new Value(data, length));
}
inline DataFrame *KVStore::get(Key &k) {
    // data is stored in local kvstore
    if (idx_ == k.node_) {
        Value *received = kv_map_.get(&k);
        return (received == nullptr) ? nullptr : new DataFrame(received->blob_);
    } else {
        // ask the network for data
        Get* g = new Get(idx_, k.node_, 1234, &k);
        sendToNeighbor(nconfig_.neighborSockets[k.node_], g->serialize());
        return nullptr;
    }
}
inline DataFrame *KVStore::waitAndGet(Key &k) {
    // data is stored in local kvstore
    if (idx_ == k.node_) {
        Value *received = kv_map_.get(&k);
        return (received == nullptr) ? nullptr : new DataFrame(received->blob_);
    } else {
        Get* g = new Get(idx_, k.node_, 1234, &k);
        sendToNeighbor(nconfig_.neighborSockets[k.node_], g->serialize());
        nconfig_.waiting = true;
        //wait for result from neighbors
        printf("waiting in waitandget!\n");
        while (nconfig_.waiting);
        DataFrame* finalResult = waitAndGetValue;
        waitAndGetValue = nullptr;
        return finalResult;
    }
}
inline void KVStore::setIndex(size_t idx) {
    idx_ = idx;
}

inline void KVStore::configure(const char* ip, int port, const char* serverIp, int serverPort) {
    nconfig_.ip_ = new String(ip);
    nconfig_.serverIp_ = new String(serverIp);
    nconfig_.port_ = port;
    nconfig_.serverPort_ = serverPort;
    nconfig_.serverBuffer = new unsigned char[BUFF_SIZE];
    nconfig_.neighborBuffer = new unsigned char[BUFF_SIZE];
    memset(nconfig_.serverBuffer, 0, BUFF_SIZE);
    memset(nconfig_.neighborBuffer, 0, BUFF_SIZE);
    nconfig_.neighborSockets = new int[TEMP_CLIENTS_MAX - 1];
    memset(nconfig_.neighborSockets, NULL, sizeof(nconfig_.neighborSockets));
    nconfig_.running = false;
    nconfig_.listenToServerThread = nullptr;
    nconfig_.listenToNeighborsThread = nullptr;
    registerWithServer();
}

inline void KVStore::configure(const char* ip, const char* serverIp, int serverPort) {
    configure(ip, serverPort, serverIp, serverPort);
}

//sends the given data to the server socket
inline void KVStore::sendToServer(unsigned char* msg) {
    if (send(nconfig_.serverSocket_, msg, message_length(msg), 0) < 0) {
        assert("Error sending data to server." && false);
    }
}

//registers this Node with the server
inline void KVStore::registerWithServer() {
    nconfig_.serverSocket_ = socket(AF_INET, SOCK_STREAM, 0);
    if (nconfig_.serverSocket_ < 0) {
        assert("Error creating socket." && false);
    }
    struct sockaddr_in servaddr;
    servaddr.sin_family = AF_INET;
    servaddr.sin_addr.s_addr = inet_addr(nconfig_.serverIp_->c_str()); 
    servaddr.sin_port = htons(nconfig_.serverPort_);

    if (connect(nconfig_.serverSocket_, (struct sockaddr *)&servaddr, sizeof(servaddr)) < 0) {
        assert("Could not connect to server." && false);
    }
    nconfig_.running = true;
    nconfig_.listenToServerThread = new std::thread(&KVStore::listenToServer, this);
    sendToServer(getRegistrationMessage());
}

//initializes peer to peer listening for other Nodes
inline void KVStore::initializePeerToPeer() {
    int opt = 1;
    nconfig_.clientSocket_ = socket(AF_INET, SOCK_STREAM, 0);
    if (nconfig_.clientSocket_ < 0) {
        assert("Error creating socket." && false);
    }
    //Allows multiple connections on a socker
    if( setsockopt(nconfig_.clientSocket_, SOL_SOCKET, SO_REUSEADDR, (char*)&opt, sizeof(opt)) < 0) {
        assert("Error allowing multiple connections." && false);
    }

    nconfig_.clientaddr.sin_family = AF_INET;
    nconfig_.clientaddr.sin_addr.s_addr = inet_addr(nconfig_.ip_->c_str());
    nconfig_.clientaddr.sin_port = htons(nconfig_.port_);

    //bind to user provided client port for listening to neighbors
    if (::bind(nconfig_.clientSocket_, (struct sockaddr *)&nconfig_.clientaddr, sizeof(nconfig_.clientaddr)) < 0) { 
        assert("Error binding client socket." && false);
    }

    //Attempt to make non-blocking
    if (ioctl(nconfig_.clientSocket_, FIONBIO, (char*)&opt) < 0) {
        assert("Failure setting to nonblocking." && false);
    }

    //Setup to listen to other nodes
    if (listen(nconfig_.clientSocket_, TEMP_CLIENTS_MAX - 1) < 0) {
        assert("Failed to listen." && false);
    }
    //p("Listening to neighbors on port ").pln(nconfig_.port_);
}

//listens to incoming and active Node connections
inline void KVStore::listenToNeighbors() {
    initializePeerToPeer();

    FD_ZERO(&nconfig_.neighborCurrentFds);
    FD_SET(nconfig_.clientSocket_, &nconfig_.neighborCurrentFds);

    while (nconfig_.running) {
        nconfig_.neighborReadFds = nconfig_.neighborCurrentFds;
        if (select(FD_SETSIZE, &nconfig_.neighborReadFds, NULL, NULL, NULL) < 0) {
            assert("Error selecting." && false);
        }
        for (int i = 0; i < FD_SETSIZE; i++) {
            if (FD_ISSET(i, &nconfig_.neighborReadFds)) {
                if (i == nconfig_.clientSocket_) {
                    //accept incoming connection
                    int addrlen = sizeof(nconfig_.clientaddr);
                    int new_socket;
                    if ((new_socket = accept(nconfig_.clientSocket_,(struct sockaddr*)&nconfig_.clientaddr, (socklen_t*)&nconfig_.clientaddr)) < 0) {
                            assert("Error accepting new socket." && false);
                    }
                    FD_SET(new_socket, &nconfig_.neighborCurrentFds);
                } else {
                    handleNodeMsg(i, readIncomingNodeMsg(i));
                }
            }
        }
    }
}

//reads the incoming msg from the given file descriptor
inline unsigned char* KVStore::readIncomingNodeMsg(int fd) {
    //clear buffer
    memset(nconfig_.neighborBuffer, 0, BUFF_SIZE);
    int bytesRead = read(fd, nconfig_.neighborBuffer, BUFF_SIZE);
    if (bytesRead < 0) {
        assert("Error reading incoming data." && false);
    }
    unsigned char* newBuff = new unsigned char[BUFF_SIZE];
    memcpy(newBuff, nconfig_.neighborBuffer, BUFF_SIZE);
    //clear buffer
    memset(nconfig_.neighborBuffer, 0, BUFF_SIZE);
    return newBuff;
}

inline void KVStore::handleDisconnect(int fd) {
    close(fd);
    FD_CLR(fd, &nconfig_.neighborCurrentFds);
}

//handles messages from other Nodes
inline void KVStore::handleNodeMsg(int fd, unsigned char* msg) {
    if (*msg != 0) {
        MsgKind kind = message_kind(msg);
        switch (kind) {
            case MsgKind::Status: {
                handleStatus(fd, msg);
                break;
            }
            case MsgKind::Get: {
                handleGet(fd, msg);
                break;
            }
            case MsgKind::Put: {
                handlePut(fd, msg);
                break;
            }
            case MsgKind::Result: {
                handleResult(fd, msg);
                break;
            }
            default: {  
                printf("char: %c\n", kind);
                assert("Unrecognized message" && false);
            }
        }
    } else {
        handleDisconnect(fd);
    }
}

inline void KVStore::sendToNeighbor(int fd, unsigned char* msg) {
    if (send(fd, msg, message_length(msg), 0) < 0) {
        assert("Error sending data to neighbor node." && false);
    }
}

//handler for status messages
inline void KVStore::handleStatus(int fd, unsigned char* msg) {
    Status* incomingStatus = new Status(msg);
    p("Received on ").p(nconfig_.ip_->c_str()).p(":").p(nconfig_.port_).p(": ").pln(incomingStatus->msg_->c_str());
    delete incomingStatus;
}

//handler for status messages
inline void KVStore::handlePut(int fd, unsigned char* msg) {
    Put* incomingPut = new Put(msg);
    //printf("New put message on %zu\n", idx_);
    //printf("put|%s|%d|%s\n",incomingPut->key_->name_->c_str(), incomingPut->key_->node_, incomingPut->value_->blob_);
    if (incomingPut->key_->node_ == idx_) {
        kv_map_.put(incomingPut->key_, incomingPut->value_);
        //pln("put in local store");
        //maybe send back ACK later to notify of successful put, get everything working first
    }
    delete incomingPut;
}

inline void KVStore::handleResult(int fd, unsigned char* msg) {
    pln("in handle result");
    Result* r = new Result(msg);
    if (r->value_ != nullptr) {
        DataFrame* result = new DataFrame(r->value_->blob_);
        waitAndGetValue = result;
        nconfig_.waiting = false;
    } else {
        assert("Error with returned value." && false);
    }
}

inline void KVStore::handleGet(int fd, unsigned char* msg) {
    Get* incomingGet = new Get(msg);
    pln("in handle get");
    if (incomingGet->key_->node_ == idx_) {
        Value* v = kv_map_.get(incomingGet->key_);
        if (v != nullptr) {
            Result* r = new Result(v->blob_);
            sendToNeighbor(nconfig_.neighborSockets[incomingGet->sender_], r->serialize());
            delete r;
        } else {
            pln("Requested value not found locally!");
        }
    }
}

//listens to the server for directory updates
inline void KVStore::listenToServer() {
    while (nconfig_.running) {
        memset(nconfig_.serverBuffer, 0, BUFF_SIZE);
        read(nconfig_.serverSocket_, nconfig_.serverBuffer, BUFF_SIZE);
        handleIncoming(nconfig_.serverBuffer);
    }
}

//handles incoming messages from the server
inline void KVStore::handleIncoming(unsigned char* data) {
    MsgKind kind = message_kind(data);
    switch (kind) {
        case MsgKind::Ack: {
            Ack* a = new Ack(data);
            handleAck(a);
            break;
        }    
        case MsgKind::Nack: {
            Nack* n = new Nack(data);
            handleNack(n);
            break;
        }
        case MsgKind::Directory: {
            updateConnections(data);
            break;
        }
        case MsgKind::Kill: {
            shutdown();
            break;
        }
    }
}

//shuts down the node
inline void KVStore::shutdown() {
    nconfig_.running = false;
    closeNodeConnections();
    closeServerConnection();
    nconfig_.listenToNeighborsThread->join();
    nconfig_.listenToServerThread->join();
    pln("Gracefully exited.");
    exit(0);
}

//closes all active connections with other Nodes
inline void KVStore::closeNodeConnections() {
    for (int i = 0; i < nconfig_.nodeDir->ports_len_; i++) {
        close(nconfig_.neighborSockets[i]);
    }
}

//closes the connection with the rendesvouz server
inline void KVStore::closeServerConnection() {
    close(nconfig_.serverSocket_);
}

//updated the node directory and opens connections with all other nodes
inline void KVStore::updateConnections(unsigned char* data) {
    nconfig_.nodeDir = new Directory(data);
    createNeighborConnections();
    //the following method was for demo/debugging purposes
    //greetAllNeighbors();
}

//Creates connections with all other nodes in the node directory
inline void KVStore::createNeighborConnections() {
    for (int i = 0; i < nconfig_.nodeDir->ports_len_; i++) {
        if (nconfig_.neighborSockets[i] == NULL) {
            if (!(nconfig_.nodeDir->addresses->vals_[i]->equals(nconfig_.ip_) && nconfig_.nodeDir->ports[i] == nconfig_.port_)) {
                nconfig_.neighborSockets[i] = socket(AF_INET, SOCK_STREAM, 0);
                if (nconfig_.neighborSockets[i] < 0) {
                    assert("Error creating socket." && false);
                }
                struct sockaddr_in neighboraddr;
                neighboraddr.sin_family = AF_INET;
                // neighboraddr.sin_addr.s_addr = inet_addr(nconfig_.ip_->c_str());
                // neighboraddr.sin_port = htons(nconfig_.port_);
                // printf("BEFORE CONNECT IP: %s:%zu\n", 
                // nconfig_.nodeDir->addresses->vals_[i]->c_str(),
                // nconfig_.nodeDir->ports[i]);
                neighboraddr.sin_addr.s_addr = inet_addr(nconfig_.nodeDir->addresses->vals_[i]->c_str());
                neighboraddr.sin_port = htons(nconfig_.nodeDir->ports[i]);
                if (connect(nconfig_.neighborSockets[i], (struct sockaddr *)&neighboraddr, sizeof(neighboraddr)) < 0) {
                    assert("Could not connect to neighbor." && false);
                }
            }
        }
    }
    // if (idx_ == 0) {
    //     printf("VALIDITY CHECK:\n");
    // for (int i = 0; i < nconfig_.nodeDir->ports_len_; i++) {
    //     printf("i: %d ", i);
    //     if (nconfig_.neighborSockets[i] == NULL)
    //         printf("is null\n");
    //     else {
    //         printf("%s:%zu\n", nconfig_.nodeDir->addresses->vals_[i]->c_str(), nconfig_.nodeDir->ports[i]);
    //     }
    // }
}

//greets all neighbors within the node directory with a status message
//Proof of concept for MVP
inline void KVStore::greetAllNeighbors() {
    for (int i = 0; i < nconfig_.nodeDir->ports_len_; i++) {
        if (nconfig_.neighborSockets[i] != NULL) {
            printf("%d: not null socket: %d\n", idx_, i);
            StrBuff* sb = new StrBuff();
            sb->c("Hello from ");
            sb->c(nconfig_.ip_->c_str());
            sb->c(":");
            sb->c(nconfig_.port_);
            sb->c("      ");
            sb->c("directed to: ");
            sb->c(nconfig_.nodeDir->ports[i]);
            Status* greetStatus = new Status(sb->get());
            sendToNeighbor(nconfig_.neighborSockets[i], greetStatus->serialize());
            delete sb;
            delete greetStatus;
        }
    }
}

//handler for Ack messages
inline void KVStore::handleAck(Ack* ack) {
    switch (ack->previous_kind) {
        case MsgKind::Register: {
            //pln("Successfully registered!");
            if (nconfig_.listenToNeighborsThread == nullptr) {
                //launch thread to listen to neighbors
                nconfig_.listenToNeighborsThread = new std::thread(&KVStore::listenToNeighbors, this);
            }
            break;
        }
        default: {
            assert("Not implemented yet" && false);
        }
    }
}

//handler for Nack messages
inline void KVStore::handleNack(Nack* nack) {
    switch (nack->previous_kind) {
        case MsgKind::Register: {
            assert("Registration failed." && false);
            break;
        }
        default: {
            assert("Not implemented yet" && false);
        }
    }
}

//creates a serialized registration message from this Node
inline unsigned char* KVStore::getRegistrationMessage() {
    Register* rMsg = new Register(nconfig_.port_, nconfig_.ip_);
    return rMsg->serialize();
}