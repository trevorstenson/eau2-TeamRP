#include "application.h"
#include "../dataframe/sor.h"
#include "../dataframe/visitor.h"
#include "../dataframe/rower.h"
#include <map>
#include "assert.h"
#include <cstring>
#include <iostream>
#include <stdio.h>

// Convenience to get the key at index
Key* get_key(size_t index, bool words) {
    string key_str = words ? "words-" : "map-";
    key_str.append(to_string(index));
    Key* k = new Key(key_str.c_str());
    return k;
}

class FileReader {
public:
    DataFrame* all_words;
    int counter = 0;
    String* temp;
    int upper_bound;

    void chunk(size_t chunks, KVStore* store) {
      for (int i = 0; i < chunks; i++) {
        Schema* schema = new Schema("S");
        DataFrame* df = new DataFrame(*schema);
        upper_bound = all_words->nrows() * (i + 1) / chunks;
        for (int j = counter; j < upper_bound; j++) {
          temp = all_words->get_string(0, counter);
          assert(temp != nullptr);
          df->set(0, df->nrows(), temp);
          counter++;
        }
        unsigned char* serial = df->serialize();
        DataFrame* test = new DataFrame(serial);
        Value* v = new Value(serial, extract_size_t(serial, 0));
        store->put(*get_key(i, true), v);
      }
    }

    /** Creates the reader and opens the file for reading.  */
    FileReader() {
        SorAdapter* sor = new SorAdapter(0, 10000, "wordcount.txt");
        all_words = sor->df_;
    }
};

class Merger : public Rower {
public:
  std::map<std::string, int> map_;

  Merger(std::map<std::string, int>& map) {
    map_ = map;
  }

  bool accept(Row& r) override {
    String* word = r.get_string(0);
    int add_amt = r.get_int(1);
    assert(word != nullptr);
    map_[std::string(word->c_str())] += add_amt;
    //std::cout << "map_[" << std::string(word->c_str()) << "] = " << map_[std::string(word->c_str())] << std::endl;
    return false;
  }

  void print_results() {
    std::cout << "\n\nWORD COUNTS:\n";

    std::map<std::string, int>::iterator it = map_.begin();
 
    // Iterate over the map using c++11 range based for loop
    for (std::pair<std::string, int> element : map_) {
      std::cout << "    " << element.first << ": " << element.second << std::endl;
    } 

    std::cout << "TOTAL WORDS: " << map_.size() << std::endl;
  }
};
 
/****************************************************************************/
class Adder : public Rower {
public:
  std::map<std::string, int> map_; // String to Num map;  Num holds an int
  DataFrame* df;

  Adder(std::map<std::string, int>& map) {
    map_ = map;
  }
 
  bool accept(Row& r) override {
    String* word = r.get_string(0);
    int add_amt = 1;
    if (r.width() > 1) add_amt = r.get_int(1);
    assert(word != nullptr);
    map_[std::string(word->c_str())] += add_amt;
    //std::cout << "map_[" << std::string(word->c_str()) << "] = " << map_[std::string(word->c_str())] << std::endl;
    return false;
  }

  void build() {
    Schema* schema = new Schema("SI");
    df = new DataFrame(*schema);

    	// Create a map iterator and point to beginning of map
	  std::map<std::string, int>::iterator it = map_.begin();
 
    // Iterate over the map using c++11 range based for loop
    int row = 0;
    for (std::pair<std::string, int> element : map_) {
      std::string word = element.first;
      int count = element.second;
      df->set(0, row, new String(word.c_str()));
      df->set(1, row, count);
      row++;
    } 
  }

  void store(size_t index, KVStore* store) {
    unsigned char* serial = df->serialize();
    DataFrame* test = new DataFrame(serial);
    Value* v = new Value(serial, extract_size_t(serial, 0));
    store->put(*get_key(index, false), v);
  }
};
 
/****************************************************************************
 * Calculate a word count for given file:
 *   1) read the data (single node)
 *   2) produce word counts per homed chunks, in parallel
 *   3) combine the results
 **********************************************************author: pmaj ****/
class WordCount : public Application {
public:
    static const size_t BUFSIZE = 1024;
    SIMap all;
    size_t nodes;
 
    WordCount(size_t idx, size_t nodes_): Application(idx) { 
      nodes = nodes_;
    }
 
  /** The master nodes reads the input, then all of the nodes count. */
  void run_() override {
    std::cout << "Starting Node " << idx_ << std::endl;
    if (idx_ == 0) {
      FileReader fr;
      fr.chunk(nodes, &kv);
    }
    local_count();
    reduce();
  }
 
  /** Compute word counts on the local node and build a data frame. */
  void local_count() {
    DataFrame* words = (kv.waitAndGet(*get_key(idx_, true)));
    p("Node ").p(idx_).pln(": starting local count...");
    map<std::string, int> map;
    Adder add(map);
    words->map(add);
    add.build();
    add.store(idx_, &kv);
    delete words;
  }
 
  /** Merge the data frames of all nodes */
  void reduce() {
    if (idx_ > 0) return;
    pln("Node 0: reducing counts...");
    DataFrame** dfs = new DataFrame*[2];
    Key* own = get_key(0, false);
    dfs[0] = kv.get(*own);
    std::cout << "Getting other nodes... " << std::endl << std::flush;
    for (size_t i = 1; i < 2; ++i) { // merge other nodes
      Key* ok = get_key(i, false);
      dfs[1] = kv.waitAndGet(*ok);
      delete ok;
    }
    merge(dfs);
    delete own;
  }
 
  void merge(DataFrame** dfs) {
    map<std::string, int> map;
    Merger m(map);
    for (int i = 0; i < 2; i++) {
      dfs[i]->map(m);
    }
    m.print_results();
  }
}; // WordcountDemo