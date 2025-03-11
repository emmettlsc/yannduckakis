#include "duckdb/execution/operator/join/physical_yannakakis_join.hpp"

#include "duckdb/common/radix_partitioning.hpp"
#include "duckdb/common/types/value_map.hpp"
#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/execution/operator/aggregate/ungrouped_aggregate_state.hpp"
#include "duckdb/function/aggregate/distributive_function_utils.hpp"
#include "duckdb/function/aggregate/distributive_functions.hpp"
#include "duckdb/function/function_binder.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/query_profiler.hpp"
#include "duckdb/optimizer/filter_combiner.hpp"
#include "duckdb/parallel/base_pipeline_event.hpp"
#include "duckdb/parallel/executor_task.hpp"
#include "duckdb/parallel/interrupt.hpp"
#include "duckdb/parallel/pipeline.hpp"
#include "duckdb/parallel/thread_context.hpp"
#include "duckdb/planner/expression/bound_aggregate_expression.hpp"
#include "duckdb/planner/expression/bound_reference_expression.hpp"
#include "duckdb/planner/filter/conjunction_filter.hpp"
#include "duckdb/planner/filter/constant_filter.hpp"
#include "duckdb/planner/filter/in_filter.hpp"
#include "duckdb/planner/filter/null_filter.hpp"
#include "duckdb/planner/filter/optional_filter.hpp"
#include "duckdb/planner/table_filter.hpp"
#include "duckdb/storage/buffer_manager.hpp"
#include "duckdb/storage/storage_manager.hpp"
#include "duckdb/storage/temporary_memory_manager.hpp"

namespace duckdb {

PhysicalYannakakisJoin::PhysicalYannakakisJoin(LogicalOperator &op, unique_ptr<PhysicalOperator> left,
    unique_ptr<PhysicalOperator> right, vector<JoinCondition> cond, 
    JoinType join_type, idx_t estimated_cardinality)
    : PhysicalComparisonJoin(op, PhysicalOperatorType::HASH_JOIN, std::move(cond), join_type, estimated_cardinality) {

    children.push_back(std::move(left));
    children.push_back(std::move(right));

    // Collect types for join conditions
    for (auto &condition : conditions) {
        condition_types.push_back(condition.left->return_type);
    }
}


//===--------------------------------------------------------------------===//
// Sink
//===--------------------------------------------------------------------===//
// YannakakisGlobalSinkState - collects all relations
class YannakakisGlobalSinkState : public GlobalSinkState {
public:
    YannakakisGlobalSinkState(const PhysicalYannakakisJoin &op, ClientContext &context)
        : context(context), op(op), finalized(false) {
        // Initialize storage for relations
        relations_data.resize(1); // Start with just one relation (the right side)
    }

    ClientContext &context;
    const PhysicalYannakakisJoin &op;
    bool finalized;
    
    // Storage for the relations data and their join keys
    vector<unique_ptr<TupleDataCollection>> relations_data;
    vector<unique_ptr<DataChunk>> relation_keys;
};

// YannakakisLocalSinkState - used by threads to collect data
class YannakakisLocalSinkState : public LocalSinkState {
public:
    YannakakisLocalSinkState(const PhysicalYannakakisJoin &op, ClientContext &context)
        : join_key_executor(context) {
        auto &allocator = BufferAllocator::Get(context);
        
        // Set up for extracting join keys from the right side
        for (auto &cond : op.conditions) {
            join_key_executor.AddExpression(*cond.right);
        }
        join_keys.Initialize(allocator, op.condition_types);
        
        // Initialize append state for the relation
        relation_data = make_uniq<TupleDataCollection>(context, op.children[1]->types);
        relation_data->InitializeAppend(append_state);
    }

    ExpressionExecutor join_key_executor;
    DataChunk join_keys;
    
    // Collection for the relation data
    unique_ptr<TupleDataCollection> relation_data;
    TupleDataAppendState append_state;
};

unique_ptr<GlobalSinkState> PhysicalYannakakisJoin::GetGlobalSinkState(ClientContext &context) const {
    return make_uniq<YannakakisGlobalSinkState>(*this, context);
}

unique_ptr<LocalSinkState> PhysicalYannakakisJoin::GetLocalSinkState(ExecutionContext &context) const {
    return make_uniq<YannakakisLocalSinkState>(*this, context.client);
}

SinkResultType PhysicalYannakakisJoin::Sink(ExecutionContext &context, DataChunk &chunk, 
    OperatorSinkInput &input) const {
auto &lstate = input.local_state.Cast<YannakakisLocalSinkState>();

// Extract join keys from the incoming chunk
lstate.join_keys.Reset();
lstate.join_key_executor.Execute(chunk, lstate.join_keys);

// Store the right-side data (we need to keep both data and keys)
lstate.relation_data->Append(lstate.append_state, chunk);

// If you need to keep the keys separately, you could store them too

return SinkResultType::NEED_MORE_INPUT;
}

SinkCombineResultType PhysicalYannakakisJoin::Combine(ExecutionContext &context, 
    OperatorSinkCombineInput &input) const {
auto &gstate = input.global_state.Cast<YannakakisGlobalSinkState>();
auto &lstate = input.local_state.Cast<YannakakisLocalSinkState>();

// Finalize the local data collection
gstate.relations_data[0] = std::move(lstate.relation_data);
 
// Move the local data to the global state
auto guard = gstate.Lock();
gstate.relations_data[0] = std::move(lstate.relation_data);

return SinkCombineResultType::FINISHED;
}



//===--------------------------------------------------------------------===//
// Finalize
//===--------------------------------------------------------------------===//

SinkFinalizeType PhysicalYannakakisJoin::Finalize(Pipeline &pipeline, Event &event, 
    ClientContext &context,
    OperatorSinkFinalizeInput &input) const {
auto &gstate = input.global_state.Cast<YannakakisGlobalSinkState>();

if (gstate.relations_data[0]->Count() == 0) {
// No data in the right relation
return EmptyResultIfRHSIsEmpty() ? SinkFinalizeType::NO_OUTPUT_POSSIBLE : SinkFinalizeType::READY;
}

// In a real implementation, we'd build a join tree first
// For this minimal version, we're just treating it as a simple chain

// Perform the two phases of Yannakakis algorithm
PerformBottomUpSemiJoin(gstate);
PerformTopDownSemiJoin(gstate);

gstate.finalized = true;
return SinkFinalizeType::READY;
}

void PhysicalYannakakisJoin::PerformBottomUpSemiJoin(YannakakisGlobalSinkState &state) const {
// Since we only have one relation in this minimal implementation,
// there's not much to do for a bottom-up phase.
// In a full implementation with multiple relations, we'd go from leaves to root

// If we had more relations, we'd do something like:
// for (idx_t i = state.relations_data.size() - 1; i > 0; i--) {
//     PerformSemiJoin(state, i, i-1); // Child to parent
// }
}

void PhysicalYannakakisJoin::PerformTopDownSemiJoin(YannakakisGlobalSinkState &state) const {
// Similar to bottom-up, with only one relation there's not much to do here
// In a full implementation, we'd go from root to leaves

// If we had more relations, we'd do something like:
// for (idx_t i = 0; i < state.relations_data.size() - 1; i++) {
//     PerformSemiJoin(state, i, i+1); // Parent to child
// }
}

void PhysicalYannakakisJoin::PerformSemiJoin(YannakakisGlobalSinkState &state, 
    idx_t source_idx, idx_t target_idx) const {
    auto &source_data = *state.relations_data[source_idx];
    auto &target_data = *state.relations_data[target_idx];
    auto &context = state.context;
    
    // Create a hash table for the source relation
    auto hash_table = make_uniq<JoinHashTable>(context, conditions, vector<LogicalType>{}, JoinType::SEMI, vector<idx_t>{});
    
    // For each chunk in the source data
    DataChunk source_chunk;
    source_data.InitializeChunk(source_chunk);
    
    DataChunk source_keys;
    source_keys.Initialize(BufferAllocator::Get(context), condition_types);
    
    // Empty payload chunk since we only care about the keys for the semi-join
    DataChunk empty_payload;
    empty_payload.SetCardinality(0);
    
    // Set up the append state for the hash table
    PartitionedTupleDataAppendState append_state;
    hash_table->GetSinkCollection().InitializeAppendState(append_state);
    
    // Scan source relation and build hash table
    TupleDataScanState source_scan_state;
    source_data.InitializeScan(source_scan_state, TupleDataPinProperties::KEEP_EVERYTHING_PINNED);
    
    while (source_data.Scan(source_scan_state, source_chunk)) {
        if (source_chunk.size() == 0) {
            continue;
        }
        
        // Extract join keys
        source_keys.Reset();
        ExecuteExpressions(context, conditions, source_chunk, source_keys, true); // Use left=true for source
        
        // Add to hash table
        hash_table->Build(append_state, source_keys, empty_payload);
    }
    
    // Finalize the hash table
    hash_table->GetSinkCollection().FlushAppendState(append_state);
    hash_table->finalized = true;
    
    // Create a new data collection for the filtered target relation
    auto filtered_target = make_uniq<TupleDataCollection>(state.context, target_data.GetLayout().GetTypes());
    TupleDataAppendState target_append_state;
    filtered_target->InitializeAppend(target_append_state);
    
    // Scan and filter the target relation
    DataChunk target_chunk;
    target_data.InitializeChunk(target_chunk);
    
    DataChunk target_keys;
    target_keys.Initialize(BufferAllocator::Get(context), condition_types);
    
    TupleDataScanState target_scan_state;
    target_data.InitializeScan(target_scan_state, TupleDataPinProperties::KEEP_EVERYTHING_PINNED);
    
    // Set up for probing the hash table
    TupleDataChunkState join_key_state;
    TupleDataCollection::InitializeChunkState(join_key_state, condition_types);
    
    while (target_data.Scan(target_scan_state, target_chunk)) {
        if (target_chunk.size() == 0) {
            continue;
        }
        
        // Extract join keys
        target_keys.Reset();
        ExecuteExpressions(context, conditions, target_chunk, target_keys, false); // Use left=false for target
        
        // Probe hash table to find matches
        JoinHashTable::ScanStructure scan_structure(*hash_table, join_key_state);
        JoinHashTable::ProbeState probe_state;
        
        // Perform probe operation
        hash_table->Probe(scan_structure, target_keys, join_key_state, probe_state);
        
        // Create a selection vector to store matching rows
        SelectionVector match_sel(target_chunk.size());
        idx_t match_count = 0;
        
        // Check which rows have matches
        for (idx_t i = 0; i < target_chunk.size(); i++) {
            if (scan_structure.found_match[i]) {
                match_sel.set_index(match_count++, i);
            }
        }
        
        if (match_count > 0) {
            // Create a filtered chunk with only the matching rows
            DataChunk filtered_chunk;
            filtered_chunk.Initialize(BufferAllocator::Get(context), target_chunk.GetTypes());
            filtered_chunk.Slice(target_chunk, match_sel, match_count);
            
            // Append to the filtered collection
            filtered_target->Append(target_append_state, filtered_chunk);
        }
    }
    
    // Finalize and replace the original target with the filtered one
    state.relations_data[target_idx] = std::move(filtered_target);
}

// Helper function to execute expressions on a chunk
void PhysicalYannakakisJoin::ExecuteExpressions(ClientContext &context, 
    const vector<JoinCondition> &conditions, 
    DataChunk &input, 
    DataChunk &result, 
    bool use_left) const {
ExpressionExecutor executor(context);

for (auto &cond : conditions) {
// Use either left or right expressions based on the parameter
if (use_left) {
executor.AddExpression(*cond.left);
} else {
executor.AddExpression(*cond.right);
}
}

executor.Execute(input, result);
}



//===--------------------------------------------------------------------===//
// Operator
//===--------------------------------------------------------------------===//

class YannakakisOperatorState : public CachingOperatorState {
public:
    explicit YannakakisOperatorState(ClientContext &context) : probe_executor(context) {
    }
    
    ExpressionExecutor probe_executor;
    DataChunk join_keys;
    DataChunk result_chunk;
    
    // Index to track our position in the join computation
    idx_t current_relation_idx = 0;
};

unique_ptr<OperatorState> PhysicalYannakakisJoin::GetOperatorState(ExecutionContext &context) const {
    auto state = make_uniq<YannakakisOperatorState>(context.client);
    auto &allocator = BufferAllocator::Get(context.client);
    
    // Set up for probing with left-side join keys
    for (auto &cond : conditions) {
        state->probe_executor.AddExpression(*cond.left);
    }
    state->join_keys.Initialize(allocator, condition_types);
    
    return std::move(state);
}

OperatorResultType PhysicalYannakakisJoin::ExecuteInternal(ExecutionContext &context, 
                    DataChunk &input, 
                    DataChunk &chunk,
                    GlobalOperatorState &gstate, 
                    OperatorState &state_p) const {
    auto &state = state_p.Cast<YannakakisOperatorState>();
    auto &sink = sink_state->Cast<YannakakisGlobalSinkState>();

    // Extract join keys from left side
    state.join_keys.Reset();
    state.probe_executor.Execute(input, state.join_keys);

    // For a minimal version, we'll just return the left-side data
    // In a full implementation, you would use the pruned relations to compute the join

    chunk.Reference(input);
    return OperatorResultType::NEED_MORE_INPUT;
}


} // namespace duckdb