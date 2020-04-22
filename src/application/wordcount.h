#include "application.h"
#include "../dataframe/sor.h"
#include "../dataframe/visitor.h"
#include "../dataframe/rower.h"
//#include <../map.h>
#include "assert.h"
#include <cstring>
#include <iostream>
#include <stdio.h>

// Convenience to get the key at index
Key* get_key(size_t index) {
    string key_str = "key-";
    key_str.append(to_string(index));
    Key* k = new Key(key_str.c_str());
    return k;
}

class FileReader {
public:
    DataFrame* all_words;
    int counter = 0;

    void chunk(size_t chunks, KVStore* store) {
      for (int i = 0; i < chunks; i++) {
        DataFrame* df = new DataFrame(*all_words);
        for (int j = counter; j < all_words->nrows() * (i + 1) / chunks; j++) {
          df->set(0, df->nrows(), all_words->get_string(0, counter));
          counter++;
        }
        unsigned char* serial = df->serialize();
        Value* v = new Value(serial, strlen((char*) serial));
        store->put(*get_key(i), v);
      }
    }

    /** Creates the reader and opens the file for reading.  */
    FileReader() {
        SorAdapter* sor = new SorAdapter(0, 10000, "wordcount.txt");
        all_words = sor->df_;
        delete sor;
    }
};
 
 
/****************************************************************************/
class Adder : public Rower {
public:
  SIMap& map_;  // String to Num map;  Num holds an int
 
  Adder(SIMap& map) : map_(map)  {}
 
  bool accept(Row& r) override {
    String* word = r.get_string(0);
    assert(word != nullptr);
    Integer* num = map_.containsKey(word) ? map_.get(word) : new Integer();
    assert(num != nullptr);
    num->val_++;
    map_.set(*word, num);
    return false;
  }
};
 
/***************************************************************************/
class Summer : public Visitor {
public:
  SIMap& map_;
  size_t i = 0;
  size_t j = 0;
  size_t seen = 0;
 
  Summer(SIMap& map) : map_(map) {}
 
  void next() {
      if (i == map_.capacity_ ) return;
      if ( j < map_.size() ) {
          j++;
          ++seen;
      } else {
          ++i;
          j = 0;
          while( i < map_.capacity_ && map_.size() == 0 )  i++;
          if (k()) ++seen;
      }
  }
 
  String* k() {
      if (i==map_.capacity_ || j == map_.size()) return nullptr;
      return (String*)(map_.values_[i]->getKey());
  }
 
  size_t v() {
      if (i == map_.capacity_ || j == map_.size()) {
          assert(false); return 0;
      }
      return ((size_t)(dynamic_cast<Integer*>(map_.values_[i]->getValue()))->get());
  }
 
  void visit(Row& r) {
      if (!k()) next();
      String & key = *k();
      size_t value = v();
      r.set(0, &key);
      r.set(1, (int) value);
      next();
  }
 
  bool done() {return seen == map_.size(); }
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
    if (idx_ == 0) {
      std::cout << "+run() -reading in on Node 0" << std::endl << flush;
      FileReader fr;
      fr.chunk(nodes, &kv);
    }
    std::cout << "+run() -local-count()" << std::endl << flush;
    local_count();
    std::cout << "+run() -reduce()" << std::endl << flush;
    reduce();
  }


 
  /** Compute word counts on the local node and build a data frame. */
  void local_count() {
    DataFrame* words = (kv.waitAndGet(*get_key(idx_)));
    p("Node ").p(idx_).pln(": starting local count...");
    SIMap map;
    Adder add(map);
    words->map(add);
    delete words;
    //Summer cnt(map);
    //delete DataFrame::fromVisitor(get_key(idx_), &kv, "SI", &cnt);
  }
 
  /** Merge the data frames of all nodes */
  void reduce() {
    if (idx_ >= 0) return;
    pln("Node 0: reducing counts...");
    SIMap map;
    Key* own = get_key(0);
    merge(kv.get(*own), map);
    for (size_t i = 1; i < 3; ++i) { // merge other nodes
      Key* ok = get_key(i);
      merge(kv.waitAndGet(*ok), map);
      delete ok;
    }
    p("Different words: ").pln(map.size());
    delete own;
  }
 
  void merge(DataFrame* df, SIMap& m) {
    Adder add(m);
    df->map(add);
    delete df;
  }
}; // WordcountDemo