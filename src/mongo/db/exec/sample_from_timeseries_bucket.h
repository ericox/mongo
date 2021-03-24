/**
 *    Copyright (C) 2021-present MongoDB, Inc.
 *
 *    This program is free software: you can redistribute it and/or modify
 *    it under the terms of the Server Side Public License, version 1,
 *    as published by MongoDB, Inc.
 *
 *    This program is distributed in the hope that it will be useful,
 *    but WITHOUT ANY WARRANTY; without even the implied warranty of
 *    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *    Server Side Public License for more details.
 *
 *    You should have received a copy of the Server Side Public License
 *    along with this program. If not, see
 *    <http://www.mongodb.com/licensing/server-side-public-license>.
 *
 *    As a special exception, the copyright holders give permission to link the
 *    code of portions of this program with the OpenSSL library under certain
 *    conditions as described in each individual source file and distribute
 *    linked combinations including the program with the OpenSSL library. You
 *    must comply with the Server Side Public License in all respects for
 *    all of the code used other than as permitted herein. If you modify file(s)
 *    with this exception, you may extend this exception to your version of the
 *    file(s), but you are not obligated to do so. If you do not wish to do so,
 *    delete this exception statement from your version. If you delete this
 *    exception statement from all source files in the program, then also delete
 *    it in the license file.
 */

#pragma once

#include "mongo/db/exec/bucket_unpacker.h"
#include "mongo/db/exec/plan_stage.h"

namespace mongo {
/**
 * This stage implements a variation on the ARHASH algorithm (see
 * https://dl.acm.org/doi/10.1145/93605.98746), by running one iteration of the ARHASH algorithm to
 * materialze a random measurement from a randomly sampled bucket once per doWork() call. It is
 * assumed that the child stage is a MultiIterator stage that iterates over a storage optimized
 * random cursor.
 */
class SampleFromTimeseriesBucket final : public PlanStage {
public:
    static const char* kStageType;

    SampleFromTimeseriesBucket(ExpressionContext* expCtx,
                               WorkingSet* ws,
                               std::unique_ptr<PlanStage> child,
                               BucketUnpacker bucketUnpacker,
                               int worksSinceLastAdvanced,
                               long long sampleSize,
                               int bucketMaxCount);

    StageType stageType() const final {
        return STAGE_SAMPLE_FROM_TIMESERIES_BUCKET;
    }

    bool isEOF() final {
        return _nSampledSoFar >= _sampleSize;
    }

    std::unique_ptr<PlanStageStats> getStats() final;

    const SpecificStats* getSpecificStats() const final {
        return &_specificStats;
    }

    PlanStage::StageState doWork(WorkingSetID* id);

private:
    /**
     * Carries the bucket _id and index for the measurement that was sampled.
     */
    struct SampledMeasurementKey {
        SampledMeasurementKey(OID bucketId, int64_t measurementIndex)
            : bucketId(bucketId), measurementIndex(measurementIndex) {}

        bool operator==(const SampledMeasurementKey& key) const {
            return this->bucketId == key.bucketId && this->measurementIndex == key.measurementIndex;
        }

        OID bucketId;
        int32_t measurementIndex;
    };

    /**
     * Computes a hash of 'SampledMeasurementKey' so measurements that have already been seen can
     * be kept track of for de-duplication after sampling.
     */
    struct SampledMeasurementKeyHasher {
        size_t operator()(const SampledMeasurementKey& s) const {
            return absl::Hash<uint64_t>{}(s.bucketId.view().read<uint64_t>()) ^
                absl::Hash<uint32_t>{}(s.bucketId.view().read<uint32_t>(8)) ^
                absl::Hash<int32_t>{}(s.measurementIndex);
        }
    };

    // Tracks which measurements have been seen so far.
    using SeenSet = stdx::unordered_set<SampledMeasurementKey, SampledMeasurementKeyHasher>;

    void materializeMeasurement(int64_t measurementIdx, WorkingSetMember* out);

    WorkingSet& _ws;
    BucketUnpacker _bucketUnpacker;
    SampleFromTimeseriesBucketStats _specificStats;

    int _worksSinceLastAdvanced;
    long long _sampleSize;
    int _bucketMaxCount;

    long long _nSampledSoFar = 0;


    // Used to de-duplicate randomly sampled measurements.
    SeenSet _seenSet;
};
}  //  namespace mongo
