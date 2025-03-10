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

#include "exprs/datasketches-functions.h"

#include "exprs/datasketches-common.h"
#include "gutil/strings/substitute.h"
#include "thirdparty/datasketches/hll.hpp"
#include "thirdparty/datasketches/theta_sketch.hpp"
#include "thirdparty/datasketches/theta_union.hpp"
#include "thirdparty/datasketches/theta_intersection.hpp"
#include "thirdparty/datasketches/theta_a_not_b.hpp"
#include "thirdparty/datasketches/kll_sketch.hpp"
#include "udf/udf-internal.h"

namespace impala {

using strings::Substitute;

BigIntVal DataSketchesFunctions::DsHllEstimate(FunctionContext* ctx,
    const StringVal& serialized_sketch) {
  if (serialized_sketch.is_null || serialized_sketch.len == 0) return BigIntVal::null();
  datasketches::hll_sketch sketch(DS_SKETCH_CONFIG, DS_HLL_TYPE);
  if (!DeserializeDsSketch(serialized_sketch, &sketch)) {
    LogSketchDeserializationError(ctx);
    return BigIntVal::null();
  }
  return sketch.get_estimate();
}

StringVal DataSketchesFunctions::DsHllEstimateBoundsAsString(
    FunctionContext* ctx, const StringVal& serialized_sketch) {
  return DsHllEstimateBoundsAsString(ctx, serialized_sketch, DS_DEFAULT_KAPPA);
}

StringVal DataSketchesFunctions::DsHllEstimateBoundsAsString(
    FunctionContext* ctx, const StringVal& serialized_sketch, const IntVal& kappa) {
  if (serialized_sketch.is_null || serialized_sketch.len == 0 || kappa.is_null)
    return StringVal::null();
  if (UNLIKELY(kappa.val < 1 || kappa.val > 3)) {
    ctx->SetError("Kappa must be 1, 2 or 3");
    return StringVal::null();
  }
  datasketches::hll_sketch sketch(DS_SKETCH_CONFIG, DS_HLL_TYPE);
  if (!DeserializeDsSketch(serialized_sketch, &sketch)) {
    LogSketchDeserializationError(ctx);
    return StringVal::null();
  }
  std::stringstream buffer;
  buffer << sketch.get_estimate() << "," << sketch.get_lower_bound(kappa.val) << ","
         << sketch.get_upper_bound(kappa.val);
  return StringStreamToStringVal(ctx, buffer);
}

StringVal DataSketchesFunctions::DsHllUnionF(FunctionContext* ctx,
    const StringVal& first_serialized_sketch, const StringVal& second_serialized_sketch) {
  // Union
  datasketches::hll_union union_sketch(DS_SKETCH_CONFIG);
  // Deserialize two sketch
  if (!first_serialized_sketch.is_null && first_serialized_sketch.len > 0) {
    datasketches::hll_sketch first_sketch(DS_SKETCH_CONFIG, DS_HLL_TYPE);
    if (!DeserializeDsSketch(first_serialized_sketch, &first_sketch)) {
      LogSketchDeserializationError(ctx);
      return StringVal::null();
    }
    union_sketch.update(first_sketch);
  }
  if (!second_serialized_sketch.is_null && second_serialized_sketch.len > 0) {
    datasketches::hll_sketch second_sketch(DS_SKETCH_CONFIG, DS_HLL_TYPE);
    if (!DeserializeDsSketch(second_serialized_sketch, &second_sketch)) {
      LogSketchDeserializationError(ctx);
      return StringVal::null();
    }
    union_sketch.update(second_sketch);
  }
  //  Result
  datasketches::hll_sketch sketch = union_sketch.get_result(DS_HLL_TYPE);
  std::stringstream serialized_input;
  sketch.serialize_compact(serialized_input);
  return StringStreamToStringVal(ctx, serialized_input);
}

StringVal DataSketchesFunctions::DsHllStringify(FunctionContext* ctx,
    const StringVal& serialized_sketch) {
  if (serialized_sketch.is_null || serialized_sketch.len == 0) return StringVal::null();
  datasketches::hll_sketch sketch(DS_SKETCH_CONFIG, DS_HLL_TYPE);
  if (!DeserializeDsSketch(serialized_sketch, &sketch)) {
    LogSketchDeserializationError(ctx);
    return StringVal::null();
  }
  string str = sketch.to_string(true, false, false, false);
  StringVal dst(ctx, str.size());
  memcpy(dst.ptr, str.c_str(), str.size());
  return dst;
}

BigIntVal DataSketchesFunctions::DsThetaEstimate(
    FunctionContext* ctx, const StringVal& serialized_sketch) {
  if (serialized_sketch.is_null || serialized_sketch.len == 0) return 0;
  try {
    // serialized_sketch may be a serialized of update_theta_sketch or
    // compact_theta_sketch
    auto sketch = datasketches::theta_sketch::deserialize(
        (void*)serialized_sketch.ptr, serialized_sketch.len);
    return sketch->get_estimate();
  } catch (const std::exception&) {
    // One reason of throwing from deserialization is that the input string is not a
    // serialized sketch.
    LogSketchDeserializationError(ctx);
    return BigIntVal::null();
  }
}

StringVal DataSketchesFunctions::DsThetaExclude(FunctionContext* ctx,
    const StringVal& first_serialized_sketch, const StringVal& second_serialized_sketch) {
  datasketches::theta_a_not_b a_not_b;
  // Deserialize two sketches
  datasketches::theta_sketch::unique_ptr first_sketch_ptr;
  if (!first_serialized_sketch.is_null && first_serialized_sketch.len > 0) {
    if (!DeserializeDsSketch(first_serialized_sketch, &first_sketch_ptr)) {
      LogSketchDeserializationError(ctx);
      return StringVal::null();
    }
  }
  datasketches::theta_sketch::unique_ptr second_sketch_ptr;
  if (!second_serialized_sketch.is_null && second_serialized_sketch.len > 0) {
    if (!DeserializeDsSketch(second_serialized_sketch, &second_sketch_ptr)) {
      LogSketchDeserializationError(ctx);
      return StringVal::null();
    }
  }
  // Note, A and B refer to the two input sketches in the order A-not-B.
  // if A is null return null.
  // if A is not null, B is null return copyA.
  // other return A-not-B.
  if (first_sketch_ptr) {
    if (!second_sketch_ptr) {
      return StringVal::CopyFrom(
          ctx, first_serialized_sketch.ptr, first_serialized_sketch.len);
    }
    // A and B are not null, call a_not_b.compute()
    auto result = a_not_b.compute(*first_sketch_ptr, *second_sketch_ptr);
    std::stringstream serialized_input;
    result.serialize(serialized_input);
    return StringStreamToStringVal(ctx, serialized_input);
  }
  return StringVal::null();
}

StringVal DataSketchesFunctions::DsThetaUnionF(FunctionContext* ctx,
    const StringVal& first_serialized_sketch, const StringVal& second_serialized_sketch) {
  datasketches::theta_union union_sketch = datasketches::theta_union::builder().build();
  // Update two sketches to theta_union
  if (!update_sketch_to_theta_union(ctx, first_serialized_sketch, union_sketch)) {
    return StringVal::null();
  }
  if (!update_sketch_to_theta_union(ctx, second_serialized_sketch, union_sketch)) {
    return StringVal::null();
  }
  //  Result
  datasketches::compact_theta_sketch sketch = union_sketch.get_result();
  std::stringstream serialized_input;
  sketch.serialize(serialized_input);
  return StringStreamToStringVal(ctx, serialized_input);
}

StringVal DataSketchesFunctions::DsThetaIntersectF(FunctionContext* ctx,
    const StringVal& first_serialized_sketch, const StringVal& second_serialized_sketch) {
  datasketches::theta_intersection intersection_sketch;
  // Update two sketches to theta_intersection
  // Note that if one of the sketches is null, null is returned.
  if (!update_sketch_to_theta_intersection(
          ctx, first_serialized_sketch, intersection_sketch)) {
    return StringVal::null();
  }
  if (!update_sketch_to_theta_intersection(
          ctx, second_serialized_sketch, intersection_sketch)) {
    return StringVal::null();
  }
  datasketches::compact_theta_sketch sketch = intersection_sketch.get_result();
  std::stringstream serialized_input;
  sketch.serialize(serialized_input);
  return StringStreamToStringVal(ctx, serialized_input);
}

FloatVal DataSketchesFunctions::DsKllQuantile(FunctionContext* ctx,
    const StringVal& serialized_sketch, const DoubleVal& rank) {
  if (serialized_sketch.is_null || serialized_sketch.len == 0) return FloatVal::null();
  if (rank.val < 0.0 || rank.val > 1.0) {
    ctx->SetError("Rank parameter should be in the range of [0,1]");
    return FloatVal::null();
  }
  datasketches::kll_sketch<float> sketch;
  if (!DeserializeDsSketch(serialized_sketch, &sketch)) {
    LogSketchDeserializationError(ctx);
    return FloatVal::null();
  }
  try {
    return sketch.get_quantile(rank.val);
  } catch (const std::exception& e) {
    ctx->SetError(Substitute("Error while getting quantile from DataSketches KLL. "
        "Message: $0", e.what()).c_str());
    return FloatVal::null();
  }
}

BigIntVal DataSketchesFunctions::DsKllN(FunctionContext* ctx,
    const StringVal& serialized_sketch) {
  if (serialized_sketch.is_null || serialized_sketch.len == 0) return BigIntVal::null();
  datasketches::kll_sketch<float> sketch;
  if (!DeserializeDsSketch(serialized_sketch, &sketch)) {
    LogSketchDeserializationError(ctx);
    return BigIntVal::null();
  }
  return sketch.get_n();
}

DoubleVal DataSketchesFunctions::DsKllRank(FunctionContext* ctx,
    const StringVal& serialized_sketch, const FloatVal& probe_value) {
  if (serialized_sketch.is_null || serialized_sketch.len == 0) return DoubleVal::null();
  datasketches::kll_sketch<float> sketch;
  if (!DeserializeDsSketch(serialized_sketch, &sketch)) {
    LogSketchDeserializationError(ctx);
    return DoubleVal::null();
  }
  return sketch.get_rank(probe_value.val);
}

StringVal DataSketchesFunctions::DsKllQuantilesAsString(FunctionContext* ctx,
    const StringVal& serialized_sketch, int num_args, const DoubleVal* args) {
  DCHECK(num_args > 0);
  if (args == nullptr) return StringVal::null();
  if (serialized_sketch.is_null || serialized_sketch.len == 0) return StringVal::null();
  if (RaiseErrorForNullOrNaNInput(ctx, num_args, args)) return StringVal::null();
  datasketches::kll_sketch<float> sketch;
  if (!DeserializeDsSketch(serialized_sketch, &sketch)) {
    LogSketchDeserializationError(ctx);
    return StringVal::null();
  }
  double quantiles_input[(unsigned int)num_args];
  for (int i = 0; i < num_args; ++i) quantiles_input[i] = args[i].val;
  try {
    std::vector<float> results = sketch.get_quantiles(quantiles_input, num_args);
    return DsKllVectorResultToStringVal(ctx, results);
  } catch(const std::exception& e) {
    ctx->SetError(Substitute("Error while getting quantiles from DataSketches KLL. "
        "Message: $0", e.what()).c_str());
    return StringVal::null();
  }
}

StringVal DataSketchesFunctions::GetDsKllPMFOrCDF(FunctionContext* ctx,
    const StringVal& serialized_sketch, int num_args, const FloatVal* args,
    PMFCDF mode) {
  DCHECK(num_args > 0);
  if (args == nullptr || args->is_null) return StringVal::null();
  if (serialized_sketch.is_null || serialized_sketch.len == 0) return StringVal::null();
  if (RaiseErrorForNullOrNaNInput(ctx, num_args, args)) return StringVal::null();
  datasketches::kll_sketch<float> sketch;
  if (!DeserializeDsSketch(serialized_sketch, &sketch)) {
    LogSketchDeserializationError(ctx);
    return StringVal::null();
  }
  float input_ranges[(unsigned int)num_args];
  for (int i = 0; i < num_args; ++i) input_ranges[i] = args[i].val;
  try {
    std::vector<double> results = (mode == PMF) ?
        sketch.get_PMF(input_ranges, num_args) : sketch.get_CDF(input_ranges, num_args);
    return DsKllVectorResultToStringVal(ctx, results);
  } catch(const std::exception& e) {
    ctx->SetError(Substitute("Error while running DataSketches KLL function. "
        "Message: $0", e.what()).c_str());
    return StringVal::null();
  }
  return StringVal::null();
}

StringVal DataSketchesFunctions::DsKllPMFAsString(FunctionContext* ctx,
    const StringVal& serialized_sketch, int num_args, const FloatVal* args) {
  return GetDsKllPMFOrCDF(ctx, serialized_sketch, num_args, args, PMF);
}

StringVal DataSketchesFunctions::DsKllCDFAsString(FunctionContext* ctx,
    const StringVal& serialized_sketch, int num_args, const FloatVal* args) {
  return GetDsKllPMFOrCDF(ctx, serialized_sketch, num_args, args, CDF);
}

StringVal DataSketchesFunctions::DsKllStringify( FunctionContext* ctx,
    const StringVal& serialized_sketch) {
  if (serialized_sketch.is_null || serialized_sketch.len == 0) return StringVal::null();
  datasketches::kll_sketch<float> sketch;
  if (!DeserializeDsSketch(serialized_sketch, &sketch)) {
    LogSketchDeserializationError(ctx);
    return StringVal::null();
  }
  string str = sketch.to_string(false, false);
  StringVal dst(ctx, str.size());
  memcpy(dst.ptr, str.c_str(), str.size());
  return dst;
}

}
