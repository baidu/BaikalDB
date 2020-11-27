// Copyright (c) 2018 Baidu, Inc. All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#ifndef ROCKSDB_LITE

#include "common.h"
#include "rocksdb/db.h"
#include "rocksdb/options.h"
#include "rocksdb/slice.h"
#include "rocksdb/utilities/transaction.h"
#include "rocksdb/utilities/transaction_db.h"
#include "rocksdb/utilities/write_batch_with_index.h"
#include <iostream>
#include <thread>

#define SNAPSHOT

using namespace rocksdb;
using namespace baikaldb;

std::string kDBPath = "/tmp/rocksdb_transaction_example";

void f1(Transaction* txn2) {
    /*
    auto s = txn2->Put("abc", "defhahaha");
    if (s.ok()) {
        std::cout << "txn2->Put ok" << std::endl;
    } else {
        std::cout << "txn2->Put failed" << std::endl;
    }
    s = txn2->Commit();
    if (s.ok()) {
      std::cout << "txn2->Commit ok" << std::endl;
    } else {
      std::cout << "txn2->Commit failed" << std::endl;
    }*/

    std::cout << "in thread" << std::endl;
    TimeCost cost;
    ReadOptions read_options;
    std::string value;

#ifdef SNAPSHOT
    read_options.snapshot = txn2->GetSnapshot();
#endif

    usleep(1000000);
    //auto s = txn2->GetForUpdate(read_options, "abc", &value);
    //DB_NOTICE("txn2->GetForUpdate");
    auto s = txn2->Put("abc", "def");
    DB_NOTICE("txn2->Put(abc, def)");
    if (s.IsNotFound()) {
        DB_NOTICE("txn2->GetForUpdate not found");
        //usleep(5000000);
    } else if (s.ok()) {
        DB_NOTICE("txn2->GetForUpdate ok");
        //usleep(5000000);
    } else if (s.IsBusy()) {
        DB_NOTICE("txn2->GetForUpdate busy: %s", s.ToString().c_str());
        txn2->Rollback();
        return;
    } else if (s.IsTimedOut()) {
        DB_NOTICE("txn2->GetForUpdate timeout: %s", s.ToString().c_str());
        txn2->Rollback();
    } else {
        DB_NOTICE("txn2->GetForUpdate error: %d, %s", s.code(), s.ToString().c_str());
        txn2->Rollback();
        return;
    }
    // s = txn2->Put("abc", "transaction2_value");
    // if (s.ok()) {
    //   DB_NOTICE("txn2->Put ok");
    // } else {
    //   DB_NOTICE("txn2->Put failed");
    // }   

    usleep(2000000);
    s = txn2->Commit();
    if (s.ok()) {
      DB_NOTICE("txn2->Commit ok");
    } else {
      DB_NOTICE("txn2->Commit failed");
    }
#ifdef SNAPSHOT
    txn2->ClearSnapshot();
#endif
    
    cost.reset();
}

void f2(Transaction* txn) {
    ReadOptions read_options;
    std::string value;
    auto s = txn->Get(read_options, "abc", &value);
    if (s.IsNotFound()) {
        DB_NOTICE("txn->Get not found");
        //usleep(5000000);
    } else if (s.ok()) {
        DB_NOTICE("txn2->Get ok");
        DB_NOTICE("val is: %s", value.c_str());
        //usleep(5000000);
    } else if (s.IsBusy()) {
        DB_NOTICE("txn->Get busy: %s", s.ToString().c_str());
        txn->Rollback();
        return;
    } else if (s.IsTimedOut()) {
        DB_NOTICE("txn->Get timeout: %s", s.ToString().c_str());
    } else {
        DB_NOTICE("txn->Get error: %d, %s", s.code(), s.ToString().c_str());
        return;
    }
}

void test_txn_recovery(TransactionDB* txn_db) {
    WriteOptions write_options;
    ReadOptions read_options;
    TransactionOptions txn_options;
    std::string value;
    write_options.disableWAL = true;

  rocksdb::ColumnFamilyHandle* cf;
  rocksdb::ColumnFamilyOptions cf_options;
  auto s = txn_db->CreateColumnFamily(cf_options, "new_cf", &cf);
  if (!s.ok()) {
      DB_WARNING("create column family failed, column family: new_cf");
      return;
  }
  DB_WARNING("CF ID: %u", cf->GetID());

	Transaction* txn1 = txn_db->BeginTransaction(write_options, txn_options);
	txn1->SetName("txn1");

	s = txn1->Put(cf, "key1", "val1");
	assert(s.ok());

	s = txn1->Put(cf, "key2", "val2");
	assert(s.ok());

	s = txn1->Put(cf, "key3", "val3");
	assert(s.ok());

	s = txn1->Delete(cf, "key2");
	assert(s.ok());

	s = txn1->Prepare();
	assert(s.ok());

	WriteBatchWithIndex* batch = txn1->GetWriteBatch();
	std::string data = batch->GetWriteBatch()->Data();

	s = txn1->Rollback();
	assert(s.ok());
	delete txn1;

  s = txn_db->DropColumnFamily(cf);
  if (!s.ok()) {
      DB_FATAL("drop column_family failed, err_message:%s", s.ToString().c_str());
      return;
  }
  s = txn_db->CreateColumnFamily(cf_options, "new_cf", &cf);
  if (!s.ok()) {
      DB_WARNING("create column family failed, column family: new_cf");
      return;
  }
  DB_WARNING("CF ID: %u", cf->GetID());

	Transaction* txn2 = txn_db->BeginTransaction(write_options, txn_options);
  txn2->SetName("txn2");
	WriteBatch batch2(data);
	s = txn2->RebuildFromWriteBatch(&batch2);
	assert(s.ok());

  DB_NOTICE("txn2 state: %d", txn2->GetState());
  s = txn2->Prepare();
  assert(s.ok());

	std::string value1;
	s = txn2->Get(read_options, cf, "key1", &value1);
	assert(s.ok());
	DB_NOTICE("value1: %s", value1.c_str());

	std::string value2;
	s = txn2->Get(read_options, cf, "key2", &value2);
	assert(s.IsNotFound());

	std::string value3;
	s = txn2->Get(read_options, cf, "key3", &value3);
	assert(s.ok());
	DB_NOTICE("value3: %s", value3.c_str());
  
  s = txn2->Commit();
  assert(s.ok());

  s = txn_db->Get(read_options, cf, "key1", &value1);
  assert(s.ok());
  DB_NOTICE("value1: %s", value1.c_str());

  s = txn_db->Get(read_options, cf, "key3", &value3);
  assert(s.ok());
  DB_NOTICE("value3: %s", value3.c_str());

	delete txn2;
	return;
}

int main(int argc, char** arvg) {
    // open DB
    Options options;
    TransactionDBOptions txn_db_options;
    options.create_if_missing = true;
    txn_db_options.transaction_lock_timeout = 10000;
    TransactionDB* txn_db = NULL;

    TimeCost cost;

    Status s = TransactionDB::Open(options, txn_db_options, kDBPath, &txn_db);
    assert(s.ok());

    WriteOptions write_options;
    ReadOptions read_options;
    TransactionOptions txn_options;
    std::string value;
    write_options.disableWAL = true;

    test_txn_recovery(txn_db);
    return 0;
    ////////////////////////////////////////////////////////
    //
    // Simple Transaction Example ("Read Committed")
    //
    ////////////////////////////////////////////////////////
#ifdef SNAPSHOT
    txn_options.set_snapshot = true;
#endif

    // Start a transaction
    Transaction* txn = txn_db->BeginTransaction(write_options, txn_options);
    assert(txn);

    Transaction* txn2 = txn_db->BeginTransaction(write_options, txn_options);
    assert(txn2);

    DB_NOTICE("BeginTransaction cost: %lu", cost.get_time());
    cost.reset();

    //std::thread transaction2(f1, txn2);

    usleep(1);

#ifdef SNAPSHOT
    read_options.snapshot = txn->GetSnapshot();
#endif
    
    usleep(2000000);

    s = txn->Put("abc", "deff");
    DB_NOTICE("s = txn->Put(abc, deff);");

    // Read a key in this transaction
    s = txn->GetForUpdate(read_options, "abc", &value);
    DB_NOTICE("txn->GetForUpdate");
    if (s.IsNotFound() || s.ok()) {
        if (s.IsNotFound()) {
            DB_NOTICE("txn->GetForUpdate not found");
        } else {
            DB_NOTICE("txn->GetForUpdate ok");
        }
        //usleep(5000000);
        //Write a key in this transaction
        // s = txn->Put("abc", "transaction1_value");
        // if (s.ok()) {
        //   DB_NOTICE("txn->Put ok");
        // } else {
        //   DB_NOTICE("txn->Put failed");
        // }
        //////////
    } else if (s.IsBusy()) {
        DB_NOTICE("txn->GetForUpdate busy: %s", s.ToString().c_str());
        txn->Rollback();
    } else if (s.IsTimedOut()) {
        DB_NOTICE("txn->GetForUpdate timeout: %s", s.ToString().c_str());
        txn->Rollback();
    } else {
        DB_NOTICE("txn->GetForUpdate error: %d, %s", s.code(), s.ToString().c_str());
        txn->Rollback();
    }

    usleep(2000000);

    BthreadCond cond;
    for (int i =0; i<500; ++i) {
        auto sub_insert = [&cond, txn] () {
            ReadOptions read_options;
            std::string value;
            auto s = txn->Get(read_options, "abc", &value);
            if (s.IsNotFound()) {
                DB_NOTICE("txn->Get not found");
                //usleep(5000000);
            } else if (s.ok()) {
                DB_NOTICE("txn2->Get ok");
                DB_NOTICE("val is: %s", value.c_str());
                //usleep(5000000);
            } else if (s.IsBusy()) {
                DB_NOTICE("txn->Get busy: %s", s.ToString().c_str());
                txn->Rollback();
            } else if (s.IsTimedOut()) {
                DB_NOTICE("txn->Get timeout: %s", s.ToString().c_str());
            } else {
                DB_NOTICE("txn->Get error: %d, %s", s.code(), s.ToString().c_str());
            }
            cond.decrease_signal();
        };
        cond.increase();
        Bthread bth;
        bth.run(sub_insert);
    }
    cond.wait();

    // Commit transaction
    s = txn->Commit();
    DB_NOTICE("txn->Commit cost: %lu", cost.get_time());
    cost.reset();
    if (s.ok()) {
      DB_NOTICE("txn->Commit ok");
    } else {
      DB_NOTICE("txn->Commit failed");
    }

#ifdef SNAPSHOT
    txn->ClearSnapshot();
#endif

    // s = txn_db->Put(write_options, "abc", "value0");
    // if (s.ok()) {
    //   std::cout << "db->Put ok" << std::endl;
    // } else {
    //   std::cout << "db->Put failed" << std::endl;
    // }

    //cost.reset();
    //////////
    // Write a key in this transaction
    // s = txn2->Put("abc", "def");
    // if (s.ok()) {
    //   std::cout << "txn2->Put ok" << std::endl;
    // } else {
    //   std::cout << "txn2->Put failed" << std::endl;
    // }

    // DB_NOTICE("txn2->Put cost: %lu", cost.get_time());
    // cost.reset();

    //transaction2.join();
    //s = txn2->Commit();
    //assert(s.ok());

    delete txn;
    delete txn2;

    read_options.snapshot = nullptr;
    // Read a key OUTSIDE this transaction. Does not affect txn.
    s = txn_db->Get(read_options, "abc", &value);
    if (s.ok()) {
        DB_NOTICE("value: %s", value.c_str());
    } else {
        DB_NOTICE("value not found");
    }

  ////////////////////////////////////////////////////////
  //
  // "Repeatable Read" (Snapshot Isolation) Example
  //   -- Using a single Snapshot
  //
  ////////////////////////////////////////////////////////

  /*
  // Set a snapshot at start of transaction by setting set_snapshot=true
  txn_options.set_snapshot = true;
  Transaction* txn = txn_db->BeginTransaction(write_options, txn_options);

  const Snapshot* snapshot = txn->GetSnapshot();

  // Write a key OUTSIDE of transaction
  s = txn_db->Put(write_options, "abc", "xyz");
  assert(s.ok());

  // Attempt to read a key using the snapshot.  This will fail since
  // the previous write outside this txn conflicts with this read.
  read_options.snapshot = snapshot;
  s = txn->GetForUpdate(read_options, "abc", &value);
  if (s.s.IsBusy()) {
    std::cout << "GetForUpdate ok" << std::endl;
  } else {
    std::cout << "GetForUpdate failed" << std::endl;
  }

  txn->Rollback();

  delete txn;
  // Clear snapshot from read options since it is no longer valid
  read_options.snapshot = nullptr;
  snapshot = nullptr;
  */

  /*
  ////////////////////////////////////////////////////////
  //
  // "Read Committed" (Monotonic Atomic Views) Example
  //   --Using multiple Snapshots
  //
  ////////////////////////////////////////////////////////

  // In this example, we set the snapshot multiple times.  This is probably
  // only necessary if you have very strict isolation requirements to
  // implement.

  // Set a snapshot at start of transaction
  txn_options.set_snapshot = true;
  txn = txn_db->BeginTransaction(write_options, txn_options);

  // Do some reads and writes to key "x"
  read_options.snapshot = txn_db->GetSnapshot();
  s = txn->Get(read_options, "x", &value);
  txn->Put("x", "x");

  // Do a write outside of the transaction to key "y"
  s = txn_db->Put(write_options, "y", "y");

  // Set a new snapshot in the transaction
  txn->SetSnapshot();
  txn->SetSavePoint();
  read_options.snapshot = txn_db->GetSnapshot();

  // Do some reads and writes to key "y"
  // Since the snapshot was advanced, the write done outside of the
  // transaction does not conflict.
  s = txn->GetForUpdate(read_options, "y", &value);
  txn->Put("y", "y");

  // Decide we want to revert the last write from this transaction.
  txn->RollbackToSavePoint();

  // Commit.
  s = txn->Commit();
  assert(s.ok());
  delete txn;
  // Clear snapshot from read options since it is no longer valid
  read_options.snapshot = nullptr;
  */

  // Cleanup
  delete txn_db;
  DestroyDB(kDBPath, options);
  return 0;
}

#endif  // ROCKSDB_LITE
