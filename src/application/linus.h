#include "wordcount.h"
#include "../dataframe/sor.h"

/**
 * The input data is a processed extract from GitHub.
 *
 * projects:  I x S   --  The first field is a project id (or pid).
 *                    --  The second field is that project's name.
 *                    --  In a well-formed dataset the largest pid
 *                    --  is equal to the number of projects.
 *
 * users:    I x S    -- The first field is a user id, (or uid).
 *                    -- The second field is that user's name.
 *
 * commits: I x I x I -- The fields are pid, uid, uid', each row represent
 *                    -- a commit to project pid, written by user uid
 *                    -- and committed by user uid',
 **/

/**************************************************************************
 * A bit set contains size() booleans that are initialize to false and can
 * be set to true with the set() method. The test() method returns the
 * value. Does not grow.
 ************************************************************************/
class Set {
public:  
  bool* vals_;  // owned; data
  size_t size_; // number of elements

  /** Creates a set of the same size as the dataframe. */ 
  Set(DataFrame* df) : Set(df->nrows()) {}

  /** Creates a set of the given size. */
  Set(size_t sz) :  vals_(new bool[sz]), size_(sz) {
      for(size_t i = 0; i < size_; i++)
          vals_[i] = false; 
  }

  ~Set() { delete[] vals_; }

  DataFrame* to_df() {
    Schema* schema = new Schema("I");
    DataFrame* df = new DataFrame(*schema);
    size_t count = 0;
    for (int i = 0; i < size_; i++) {
      if (vals_[i]) {
        df->set(0, count, i);
        count++;
      }
    }
    return df; 
  }

  /** Add idx to the set. If idx is out of bound, ignore it.  Out of bound
   *  values can occur if there are references to pids or uids in commits
   *  that did not appear in projects or users.
   */
  void set(size_t idx) {
    if (idx >= size_ ) return; // ignoring out of bound writes
    vals_[idx] = true;       
  }

  /** Is idx in the set?  See comment for set(). */
  bool test(size_t idx) {
    if (idx >= size_) return false; // ignoring out of bound reads
    return vals_[idx];
  }

  size_t size() { return size_; }

  size_t taggedSize() {
    size_t total = 0;
    for (int i = 0; i < size(); i++) {
      if (test(i)) total++;
    }
    return total;
  }

  /** Performs set union in place. */
  void union_(Set& from) {
    for (size_t i = 0; i < from.size_; i++) 
      if (from.test(i))
	      set(i);
  }
};


// /*******************************************************************************
//  * A SetUpdater is a reader that gets the first column of the data frame and
//  * sets the corresponding value in the given set.
//  ******************************************************************************/
class SetUpdater : public Rower {
public:
  Set& set_; // set to update
  
  SetUpdater(Set& set): set_(set) {}

  /** Assume a row with at least one column of type I. Assumes that there
   * are no missing. Reads the value and sets the corresponding position.
   * The return value is irrelevant here. */
  bool accept(Row & r) { set_.set(r.get_int(0));  return false; }

};

// /*****************************************************************************
//  * A SetWriter copies all the values present in the set into a one-column
//  * dataframe. The data contains all the values in the set. The dataframe has
//  * at least one integer column.
//  ****************************************************************************/
class SetWriter: public Visitor {
public:
  Set& set_; // set to read from
  int i_ = 0;  // position in set

  SetWriter(Set& set): set_(set) { }

  /** Skip over false values and stop when the entire set has been seen */
  bool done() {
    while (i_ < set_.size_ && set_.test(i_) == false) ++i_;
    return i_ == set_.size_;
  }

  void visit(Row & row) { 
    row.set(0, i_++); 
  }
};

// /***************************************************************************
//  * The ProjectTagger is a reader that is mapped over commits, and marks all
//  * of the projects to which a collaborator of Linus committed as an author.
//  * The commit dataframe has the form:
//  *    pid x uid x uid
//  * where the pid is the identifier of a project and the uids are the
//  * identifiers of the author and committer. If the author is a collaborator
//  * of Linus, then the project is added to the set. If the project was
//  * already tagged then it is not added to the set of newProjects.
//  *************************************************************************/
class ProjectsTagger : public Rower {
public:
  Set& uSet; // set of collaborator 
  Set& pSet; // set of projects of collaborators
  Set newProjects;  // newly tagged collaborator projects

  ProjectsTagger(Set& uSet, Set& pSet, DataFrame* proj):
    uSet(uSet), pSet(pSet), newProjects(proj) {}

  /** The data frame must have at least two integer columns. The newProject
   * set keeps track of projects that were newly tagged (they will have to
   * be communicated to other nodes). */
  bool accept(Row & row) override {
    int pid = row.get_int(0);
    int uid = row.get_int(1);
    if (uSet.test(uid)) 
      if (!pSet.test(pid)) {
    	  pSet.set(pid);
        newProjects.set(pid);
      }
    return false;
  }
};

// /***************************************************************************
//  * The UserTagger is a reader that is mapped over commits, and marks all of
//  * the users which commmitted to a project to which a collaborator of Linus
//  * also committed as an author. The commit dataframe has the form:
//  *    pid x uid x uid
//  * where the pid is the idefntifier of a project and the uids are the
//  * identifiers of the author and committer. 
//  *************************************************************************/
class UsersTagger : public Rower {
public:
  Set& pSet;
  Set& uSet;
  Set newUsers;

  UsersTagger(Set& pSet,Set& uSet, DataFrame* users):
    pSet(pSet), uSet(uSet), newUsers(users->nrows()) { }

  bool accept(Row & row) override {
    int pid = row.get_int(0);
    int uid = row.get_int(1);
    if (pSet.test(pid)) 
      if(!uSet.test(uid)) {
        uSet.set(uid);
        newUsers.set(uid);
      }
    return false;
  }
};

/*************************************************************************
 * This computes the collaborators of Linus Torvalds.
 * is the linus example using the adapter.  And slightly revised
 *   algorithm that only ever trades the deltas.
 **************************************************************************/
class Linus : public Application {
public:
  int DEGREES = 5;  // How many degrees of separation form linus?
  int LINUS = 1;   // The uid of Linus (offset in the user df) //WAS 4967 - changed to heroku
  const char* PROJ = "datasets/projects.ltgt";
  const char* USER = "datasets/users.ltgt";
  const char* COMM = "datasets/commits.test";
  DataFrame* projects; //  pid x project name
  DataFrame* users;  // uid x user name
  DataFrame* commits;  // pid x uid x uid 
  Set* uSet; // Linus' collaborators
  Set* pSet; // projects of collaborators

  Linus(size_t idx): Application(idx) {}
  //Linus(size_t idx, NetworkIfc& net): Application(idx, net) {}

  /** Compute DEGREES of Linus.  */
  void run_() override {
    readInput();
    for (size_t i = 0; i < DEGREES; i++) step(i);
  }

  /** Node 0 reads three files, cointainng projects, users and commits, and
   *  creates thre dataframes. All other nodes wait and load the three
   *  dataframes. Once we know the size of users and projects, we create
   *  sets of each (uSet and pSet). We also output a data frame with a the
   *  'tagged' users. At this point the dataframe consists of only
   *  Linus. **/
  void readInput() {
    Key pK("projs");
    Key uK("usrs");
    Key cK("comts");
    
    if (idx_ == 0) {
      pln("Reading...");

      SorAdapter* sor_projects = new SorAdapter(0, UINT32_MAX, strdup(PROJ));
      projects = sor_projects->df_; //DataFrame::fromFile(PROJ, pK.clone(), &kv);
      unsigned char* blob = projects->serialize();
      kv.put(*dynamic_cast<Key*>(pK.clone()), new Value(blob, strlen((char*)blob)));
      p("    ").p(projects->nrows()).pln(" projects");

      SorAdapter* sor_users = new SorAdapter(0, UINT32_MAX, strdup(USER));
      users = sor_users->df_; //DataFrame::fromFile(USER, uK.clone(), &kv);
      blob = users->serialize();
      kv.put(*dynamic_cast<Key*>(uK.clone()), new Value(blob, strlen((char*)blob)));
      p("    ").p(users->nrows()).pln(" users");

      SorAdapter* sor_commits = new SorAdapter(0, UINT32_MAX, strdup(COMM));
      commits = sor_commits->df_; //DataFrame::fromFile(COMM, cK.clone(), &kv);
      blob = commits->serialize();
      kv.put(*dynamic_cast<Key*>(cK.clone()), new Value(blob, strlen((char*) blob)));
      p("    ").p(commits->nrows()).pln(" commits");
       // This dataframe contains the id of Linus.
       //delete
      DataFrame::fromScalar(new Key("users-0-0"), &kv, LINUS);
    } else {
       projects = dynamic_cast<DataFrame*>(kv.waitAndGet(pK));
       users = dynamic_cast<DataFrame*>(kv.waitAndGet(uK));
       commits = dynamic_cast<DataFrame*>(kv.waitAndGet(cK));
    }
    uSet = new Set(users);
    pSet = new Set(projects);
 }

 /** Performs a step of the linus calculation. It operates over the three
  *  datafrrames (projects, users, commits), the sets of tagged users and
  *  projects, and the users added in the previous round. */
  void step(int stage) {
    p("Stage ").pln(stage);
    // Key of the shape: users-stage-0
    Key uK(StrBuff("users-").c(stage).c("-0").get());
    // A df with all the users added on the previous round
    DataFrame* newUsers = dynamic_cast<DataFrame*>(kv.waitAndGet(uK));    
    Set delta(users);
    SetUpdater upd(delta);  
    newUsers->map(upd); // all of the new users are copied to delta.
    delete newUsers;
    ProjectsTagger ptagger(delta, *pSet, projects);
    commits->map(ptagger); //local_map(ptagger); // marking all projects touched by delta
    merge(ptagger.newProjects, "projects-", stage);
    pSet->union_(ptagger.newProjects); // 
    UsersTagger utagger(ptagger.newProjects, *uSet, users);
    commits->map(utagger);
    merge(utagger.newUsers, "users-", stage + 1);
    uSet->union_(utagger.newUsers);
    p("    after stage ").p(stage).pln(":");
    p("        tagged projects: ").pln(pSet->taggedSize());//->size());
    p("        tagged users: ").pln(uSet->taggedSize());//size());
  }

  /** Gather updates to the given set from all the nodes in the systems.
   * The union of those updates is then published as dataframe.  The key
   * used for the otuput is of the form "name-stage-0" where name is either
   * 'users' or 'projects', stage is the degree of separation being
   * computed.
   */ 
  void merge(Set& set, char const* name, int stage) {
    if (this_node() == 0) {
      //for (size_t i = 1; i < arg.num_nodes; ++i) {
      for (size_t i = 1; i < 1; ++i) {
        Key nK(StrBuff(name).c(stage).c("-").c(i).get());
        DataFrame* delta = dynamic_cast<DataFrame*>(kv.waitAndGet(nK));
        p("    received delta of ").p(delta->nrows())
          .p(" elements from node ").pln(i);
        SetUpdater upd(set);
        delta->map(upd);
        delete delta;
      }
      p("    storing ").p(set.size()).pln(" merged elements");
      //SetWriter writer(set);
      Key k(StrBuff(name).c(stage).c("-0").get());
      DataFrame* df = set.to_df();
      unsigned char* serial = df->serialize();
      kv.put(*dynamic_cast<Key*>(k.clone()), new Value(serial, strlen((char*)serial)));
      //delete DataFrame::fromVisitor(&k, &kv, "I", &writer);
    } else {
      p("    sending ").p(set.size()).pln(" elements to master node");
      SetWriter writer(set);
      Key k(StrBuff(name).c(stage).c("-").c(idx_).get());
      delete DataFrame::fromVisitor(&k, &kv, "I", &writer);
      Key mK(StrBuff(name).c(stage).c("-0").get());
      DataFrame* merged = dynamic_cast<DataFrame*>(kv.waitAndGet(mK));
      p("    receiving ").p(merged->nrows()).pln(" merged elements");
      SetUpdater upd(set);
      merged->map(upd);
      delete merged;
    }
  }
}; // Linus