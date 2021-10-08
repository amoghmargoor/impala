// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#include <cstdlib>
#include <cstdio>
#include <vector>
#include <boost/scoped_ptr.hpp>

#include "common/init.h"
#include "runtime/descriptors.h"
#include "runtime/mem-pool.h"
#include "runtime/mem-tracker.h"
#include "runtime/tuple-row.h"
#include "service/fe-support.h"
#include "service/frontend.h"
#include "scratch-tuple-batch.h"
#include "testutil/desc-tbl-builder.h"
#include "testutil/gtest-util.h"

#include "common/names.h"

using namespace impala;

namespace impala {

scoped_ptr<Frontend> fe;

class ScratchTupleBatchTest : public testing::Test {
 public:
  ScratchTupleBatchTest() {}

 protected:
  MemTracker tracker_;
  ObjectPool pool_;
  RowDescriptor* desc_;

  virtual void SetUp() {
    DescriptorTblBuilder builder(fe.get(), &pool_);
    builder.DeclareTuple() << TYPE_INT;
    DescriptorTbl* desc_tbl = builder.Build();
    vector<bool> nullable_tuples(1, false);
    vector<TTupleId> tuple_id(1, (TTupleId) 0);
    desc_ = pool_.Add(new RowDescriptor(*desc_tbl, tuple_id, nullable_tuples));
  }
};

// This tests checks conversion of 'selected_rows' to 'ScratchMicroBatch';
TEST_F(ScratchTupleBatchTest, TestGetMicroBatches) {
  const int BATCH_SIZE = 1024;
  scoped_ptr<ScratchTupleBatch> scratch_batch(new ScratchTupleBatch(*desc_, BATCH_SIZE, &tracker_));
  scratch_batch->num_tuples = BATCH_SIZE;
  // set every 16th row as selected.
  for (int batch_idx = 0; batch_idx < 1024; ++batch_idx) {
    scratch_batch->selected_rows[batch_idx] = batch_idx % 16 == 0 ? true : false;
  }
  // Creates just one micro batch as skip length is bigger than 16 (gap between any
  // two consecutive true values).
  ScratchMicroBatch micro_batches[BATCH_SIZE];
  EXPECT_EQ(scratch_batch->GetMicroBatches(micro_batches, 20 /*Skip Length*/), 1);
  EXPECT_EQ(micro_batches[0].start, 0);
  EXPECT_EQ(micro_batches[0].end, 1008);
  EXPECT_EQ(micro_batches[0].length, 1009);

  // Creates one micro batch for every true value
  EXPECT_EQ(scratch_batch->GetMicroBatches(micro_batches, 5 /*Skip Length*/), 64);
  for (int batch_idx = 0; batch_idx < 64; ++batch_idx) {
    EXPECT_EQ(micro_batches[batch_idx].start, batch_idx * 16);
    EXPECT_EQ(micro_batches[batch_idx].end, batch_idx * 16);
    EXPECT_EQ(micro_batches[batch_idx].length, 1);
  }
}
}

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  InitCommonRuntime(argc, argv, true, impala::TestInfo::BE_TEST);
  InitFeSupport();
  fe.reset(new Frontend());
  return RUN_ALL_TESTS();
}
