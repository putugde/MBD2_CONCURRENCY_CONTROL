// Author: Kun Ren (kun.ren@yale.edu)
// Modified by Daniel Abadi

#include "txn/mvcc_storage.h"

// Init the storage
void MVCCStorage::InitStorage() {
  for (int i = 0; i < 1000000;i++) {
    Write(i, 0, 0);
    Mutex* key_mutex = new Mutex();
    mutexs_[i] = key_mutex;
  }
}

// Free memory.
MVCCStorage::~MVCCStorage() {
  for (unordered_map<Key, deque<Version*>*>::iterator it = mvcc_data_.begin();
       it != mvcc_data_.end(); ++it) {
    delete it->second;          
  }
  
  mvcc_data_.clear();
  
  for (unordered_map<Key, Mutex*>::iterator it = mutexs_.begin();
       it != mutexs_.end(); ++it) {
    delete it->second;          
  }
  
  mutexs_.clear();
}

// Lock the key to protect its version_list. Remember to lock the key when you read/update the version_list 
void MVCCStorage::Lock(Key key) {
  mutexs_[key]->Lock();
}

// Unlock the key.
void MVCCStorage::Unlock(Key key) {
  mutexs_[key]->Unlock();
}

// MVCC Read
bool MVCCStorage::Read(Key key, Value* result, int txn_unique_id) {
  // CPSC 438/538:
  //
  // Implement this method!
  
  // Hint: Iterate the version_lists and return the verion whose write timestamp
  // (version_id) is the largest write timestamp less than or equal to txn_unique_id.

  if (mvcc_data_.count(key)) {
    int largest_write_ts = -1;
    deque<Version*>* version_list = mvcc_data_[key];
    for (deque<Version*>::iterator it = version_list->begin(); it != version_list->end(); ++it) {
      int write_ts = (*it)->version_id_; 
      if (write_ts <= txn_unique_id && write_ts > largest_write_ts) {
        *result = (*it)->value_;
        largest_write_ts = write_ts;
        if (txn_unique_id > (*it)->max_read_id_) {
          (*it)->max_read_id_ = txn_unique_id;
        }
      }  
    }
    if (largest_write_ts == -1) {
      return false;
    }
    return true;
  } else {
    return false;
  }
}


// Check whether apply or abort the write
bool MVCCStorage::CheckWrite(Key key, int txn_unique_id) {
  // CPSC 438/538:
  //
  // Implement this method!
  
  // Hint: Before all writes are applied, we need to make sure that each write
  // can be safely applied based on MVCC timestamp ordering protocol. This method
  // only checks one key, so you should call this method for each key in the
  // write_set. Return true if this key passes the check, return false if not. 
  // Note that you don't have to call Lock(key) in this method, just
  // call Lock(key) before you call this method and call Unlock(key) afterward.
  
  if (mvcc_data_.count(key)) {
    deque<Version*>* version_list = mvcc_data_[key];
    for(deque<Version*>::iterator it = version_list->begin() ; it != version_list->end() ; ++it) {
    Version* version = *it;
      if (version == NULL) {
        if (version->version_id_ > txn_unique_id || version->max_read_id_ > txn_unique_id ) {
          return false;
        }
      }
    }
    return true;
  } else {
    return false;
  }
}

// MVCC Write, call this method only if CheckWrite return true.
void MVCCStorage::Write(Key key, Value value, int txn_unique_id) {
  // CPSC 438/538:
  //
  // Implement this method!
  
  // Hint: Insert a new version (malloc a Version and specify its value/version_id/max_read_id)
  // into the version_lists. Note that InitStorage() also calls this method to init storage. 
  // Note that you don't have to call Lock(key) in this method, just
  // call Lock(key) before you call this method and call Unlock(key) afterward.

  Version* version = (Version*) malloc (sizeof(Version));
  version->value_ = value;
  version->max_read_id_ = 0;
  version->version_id_ = txn_unique_id;

  deque<Version*>* version_list;
  if (mvcc_data_.count(key)) {
    version_list = mvcc_data_[key];
  } else {
    version_list = new deque<Version*>();
    mvcc_data_[key] = version_list;
  }
  
  version_list->push_back(version);
}


