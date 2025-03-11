//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/execution/operator/join/physical_yannakakis_join.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/value_operations/value_operations.hpp"
#include "duckdb/execution/join_hashtable.hpp"
#include "duckdb/execution/operator/join/physical_comparison_join.hpp"
#include "duckdb/execution/physical_operator.hpp"
#include "duckdb/planner/operator/logical_join.hpp"

namespace duckdb {

class YannakakisGlobalSinkState;

//! PhysicalYannakakisJoin represents a Yannakakis algorithm join between tables
class PhysicalYannakakisJoin : public PhysicalComparisonJoin {
public:
    static constexpr const PhysicalOperatorType TYPE = PhysicalOperatorType::HASH_JOIN; // Reuse HASH_JOIN type for now

public:
    PhysicalYannakakisJoin(LogicalOperator &op, unique_ptr<PhysicalOperator> left, unique_ptr<PhysicalOperator> right,
                         vector<JoinCondition> cond, JoinType join_type, idx_t estimated_cardinality);

    //! The types of the join keys
    vector<LogicalType> condition_types;

public:
    InsertionOrderPreservingMap<string> ParamsToString() const override;

public:
    // Operator Interface
    unique_ptr<OperatorState> GetOperatorState(ExecutionContext &context) const override;

    bool ParallelOperator() const override {
        return false; // TODO: make true eventually, false for non-parallel version
    }

protected:
    // CachingOperator Interface
    OperatorResultType ExecuteInternal(ExecutionContext &context, DataChunk &input, DataChunk &chunk,
                                     GlobalOperatorState &gstate, OperatorState &state) const override;

    // Source interface
    unique_ptr<GlobalSourceState> GetGlobalSourceState(ClientContext &context) const override;
    unique_ptr<LocalSourceState> GetLocalSourceState(ExecutionContext &context,
                                                   GlobalSourceState &gstate) const override;
    SourceResultType GetData(ExecutionContext &context, DataChunk &chunk, OperatorSourceInput &input) const override;

    bool IsSource() const override {
        return true;
    }

    bool ParallelSource() const override {
        return false; // TODO: make true eventually, false for non-parallel version
    }

public:
    // Sink Interface
    unique_ptr<GlobalSinkState> GetGlobalSinkState(ClientContext &context) const override;

    unique_ptr<LocalSinkState> GetLocalSinkState(ExecutionContext &context) const override;
    SinkResultType Sink(ExecutionContext &context, DataChunk &chunk, OperatorSinkInput &input) const override;
    SinkCombineResultType Combine(ExecutionContext &context, OperatorSinkCombineInput &input) const override;
    SinkFinalizeType Finalize(Pipeline &pipeline, Event &event, ClientContext &context,
                            OperatorSinkFinalizeInput &input) const override;

    bool IsSink() const override {
        return true;
    }
    bool ParallelSink() const override {
        return false; // TODO: make true eventually, false for non-parallel version
    }

private:
    // Yannakakis-specific methods
    void PerformBottomUpSemiJoin(YannakakisGlobalSinkState &state) const;
    void PerformTopDownSemiJoin(YannakakisGlobalSinkState &state) const;
    void PerformSemiJoin(YannakakisGlobalSinkState &state, idx_t source_idx, idx_t target_idx) const;
    
    // Helper function for extracting join keys
    void ExecuteExpressions(ClientContext &context, const vector<JoinCondition> &conditions, 
                         DataChunk &input, DataChunk &result, bool use_left = true) const;
};

} // namespace duckdb