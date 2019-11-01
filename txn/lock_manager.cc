// Author: Alexander Thomson (thomson@cs.yale.edu)
// Modified by: Kun Ren (kun.ren@yale.edu)
//
// Lock manager implementing deterministic two-phase locking as described in
// 'The Case for Determinism in Database Systems'.

#include "txn/lock_manager.h"

LockManager::~LockManager() {
  // Destructor
  auto itr = lock_table_.begin();
  while (itr != lock_table_.end()){
    delete itr->second;
    itr++;
  }
}


LockManagerA::LockManagerA(deque<Txn*>* ready_txns) {
  ready_txns_ = ready_txns;
}

bool LockManagerA::WriteLock(Txn* txn, const Key& key) {
  LockRequest lockReq(EXCLUSIVE, txn);
  deque<LockRequest> *lockReqDeque = lock_table_[key];
  if (!lockReqDeque) {
    lockReqDeque = new deque<LockRequest>();
    lock_table_[key] = lockReqDeque;
  }
  bool empty = lockReqDeque->empty();
  lockReqDeque->push_back(lockReq);
  if (!empty) {
    txn_waits_[txn]++;
  }
  return empty;
}

bool LockManagerA::ReadLock(Txn* txn, const Key& key) {
  // Since Part 1A implements ONLY exclusive locks, calls to ReadLock can
  // simply use the same logic as 'WriteLock'.
  return WriteLock(txn, key);
}

void LockManagerA::Release(Txn* txn, const Key& key) {
  deque<LockRequest> *lockReqDeque = lock_table_[key];
  if (!lockReqDeque) {
    lockReqDeque = new deque<LockRequest>();
    lock_table_[key] = lockReqDeque;
  }
  bool resourceFree = false;
  auto itr = lockReqDeque->begin();
  while (itr < lockReqDeque->end()){
    if (itr->txn_ == txn) {
        lockReqDeque->erase(itr);
        resourceFree = true;
        break;
    }
    itr++;
  }

  if (!lockReqDeque->empty() && resourceFree) {
    LockRequest nextLockReq = lockReqDeque->front();
    if (--txn_waits_[nextLockReq.txn_] == 0) {
      ready_txns_->push_back(nextLockReq.txn_);
      txn_waits_.erase(nextLockReq.txn_);
    }
  }
}

LockMode LockManagerA::Status(const Key& key, vector<Txn*>* owners) {
  deque<LockRequest> *lockReqDeque = lock_table_[key];
  if (!lockReqDeque) {
    lockReqDeque = new deque<LockRequest>();
    lock_table_[key] = lockReqDeque;
  }

  if (lockReqDeque->empty()) {
    return UNLOCKED;
  } else {
    vector<Txn*> _owners;
    _owners.push_back(lockReqDeque->front().txn_);
    *owners = _owners;
    return EXCLUSIVE;
  }
}



LockManagerB::LockManagerB(deque<Txn*>* ready_txns) {
  ready_txns_ = ready_txns;
}

bool LockManagerB::WriteLock(Txn* txn, const Key& key) {
  // CPSC 438/538:
  //
  // Implement this method!
  return true;
}

bool LockManagerB::ReadLock(Txn* txn, const Key& key) {
  // CPSC 438/538:
  //
  // Implement this method!
  return true;
}

void LockManagerB::Release(Txn* txn, const Key& key) {
  // CPSC 438/538:
  //
  // Implement this method!
}

LockMode LockManagerB::Status(const Key& key, vector<Txn*>* owners) {
  // CPSC 438/538:
  //
  // Implement this method!
  return UNLOCKED;
}

