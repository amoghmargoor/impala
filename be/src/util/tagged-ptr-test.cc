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

#include <string>
#include "testutil/gtest-util.h"
#include "util/tagged-ptr.h"

namespace impala {

class TaggedPtrTest {
 public:
  int id;
  TaggedPtrTest(int i, std::string s) {
    id = i;
    str = s;
  }

  std::string GetString() { return str; }

  bool operator ==(const TaggedPtrTest& other) {
    return id == other.id && str == other.str;
  }

 private:
  std::string str;
};

TaggedPtr<TaggedPtrTest> MakeTaggedPtr(int a1, std::string s1) {
  return TaggedPtr<TaggedPtrTest>::make_tagptr(a1, s1);
}

union TestData {
  int x;
  float y;
  const char * s;
};

struct TestBucket; // Forward Declaration.

union TestBucketData {
  TestData data;
  TestBucket* next;
};

class TaggedBucketData: public TaggedPtr<TestBucketData, false> {
 public:
  bool IsData() { return IsTagBitSet0(); }
  void SetIsData() { SetTagBit0(); }
  void UnSetIsData() { UnSetTagBit0(); }
  TestBucketData* GetData() {
    return GetPtr();
  }
  void SetBucketData(uintptr_t address) {
    SetPtr((TestBucketData *) address);
  }
  // In destructor reset the pointer so that base contructor doesn't free it.
  ~TaggedBucketData() {}
};

struct TestBucket {
  int id;
  TaggedBucketData bucketData;
};

TEST(TaggedPtrTest, Simple) {
  auto ptr = MakeTaggedPtr(3, "test1");
  // Test for non-null
  EXPECT_FALSE(!ptr);

  // Test dereference operator
  EXPECT_EQ((*ptr).id, 3);
  EXPECT_EQ(ptr->GetString(), std::string("test1"));

  // Test initial tag
  EXPECT_EQ(ptr.GetTag(), 0);

  // Set Tag and check
  ptr.SetTagBit0();
  ptr.SetTagBit1();
  EXPECT_TRUE(ptr.IsTagBitSet0());
  EXPECT_TRUE(ptr.IsTagBitSet1());
  EXPECT_FALSE(ptr.IsTagBitSet2());
  EXPECT_EQ(ptr.GetTag(), 96);

  // Unset Tag
  ptr.UnSetTagBit0();
  EXPECT_FALSE(ptr.IsTagBitSet0());
  EXPECT_EQ(ptr.GetTag(), 32);

  // Move Semantics
  auto ptr_move1 = std::move(ptr);
  EXPECT_FALSE(!ptr_move1);
  EXPECT_TRUE(!ptr);
  TaggedPtr<TaggedPtrTest> ptr_move2;
  EXPECT_TRUE(!ptr_move2);
  ptr_move2 = std::move(ptr_move1);
  EXPECT_FALSE(!ptr_move2);
}

TEST(TaggedPtrTest, Comparision) {
  auto ptr1 = MakeTaggedPtr(3, "test1");
  auto ptr2 = MakeTaggedPtr(3, "test2");
  auto ptr3 = MakeTaggedPtr(3, "test1");
  ptr1.SetTagBit1();
  ptr1.SetTagBit2();
  ptr3.SetTagBit1();
  ptr3.SetTagBit2();
  EXPECT_FALSE(ptr1 == ptr3);
  EXPECT_TRUE(ptr1 != ptr2);
  EXPECT_TRUE(*ptr1 == *ptr3);
  EXPECT_FALSE(*ptr1 == *ptr2);
}
TEST(TaggedPtrTest, Complex) {
  // Check if tag bits are retained on setting Data
  TestBucket tagTest;
  tagTest.bucketData.SetIsData();
  tagTest.bucketData.SetTagBit1();
  TestBucketData tag_bucket_data;
  tag_bucket_data.data.s = "TagTest";
  tagTest.bucketData.SetBucketData((uintptr_t) &tag_bucket_data);
  EXPECT_TRUE(tagTest.bucketData.IsData());
  EXPECT_TRUE(tagTest.bucketData.IsTagBitSet1());
  EXPECT_EQ(tagTest.bucketData.GetData()->data.s, "TagTest");
  // set to null and check
  tagTest.bucketData.SetBucketData(0);
  EXPECT_TRUE(tagTest.bucketData.IsNull());
  EXPECT_TRUE(tagTest.bucketData.IsData());
  EXPECT_TRUE(tagTest.bucketData.IsTagBitSet1());

  // Creating two buckets bucket1 and bucket2 and linking bucket 2 to bucket1.
  TestBucket bucket1;
  bucket1.id = 1;
  TestBucketData bucket_data;
  bucket_data.data.s = "testString";
  TestBucket bucket2;
  bucket2.id = 2;
  bucket2.bucketData.SetBucketData((uintptr_t) &bucket_data);
  bucket2.bucketData.SetIsData();
  TestBucketData bucket_data1;
  bucket_data1.next = &bucket2;
  bucket1.bucketData.SetBucketData((uintptr_t) &bucket_data1);

  EXPECT_FALSE(bucket1.bucketData.IsData());
  auto first_bd = bucket1.bucketData.GetData();
  EXPECT_EQ(first_bd->next->id, 2);
  auto second_bd = first_bd->next->bucketData.GetData();
  EXPECT_EQ(second_bd->data.s, "testString");
}
};
