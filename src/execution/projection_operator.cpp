#include "execution/projection_operator.hpp"

namespace babydb {

static std::vector<std::unique_ptr<Projection>> TransToVec(std::unique_ptr<Projection> &&projection) {
    std::vector<std::unique_ptr<Projection>> result;
    result.push_back(std::move(projection));
    return result;
}

ProjectionOperator::ProjectionOperator(const ExecutionContext &exec_ctx,
                                       const std::shared_ptr<Operator> &probe_child_operator,
                                       std::vector<std::unique_ptr<Projection>> &&projections,
                                       bool update_in_place)
    : Operator(exec_ctx, {probe_child_operator}) {
    for (auto &projection_function : projections_) {
        if (projection_function == nullptr) {
            throw std::logic_error("ProjectionOperator: projection function is nullptr");
        }
    }
    if (update_in_place) {
        output_schema_ = child_operators_[0]->GetOutputSchema();
        projections_.resize(output_schema_.size());

        for (auto &projection_function : projections) {
            auto key_attr = output_schema_.GetKeyAttr(projection_function->output_name_);
            if (projections_[key_attr] != nullptr) {
                throw std::logic_error("ProjectionOperator: Duplicate projection function's output");
            }
            projections_[key_attr] = std::move(projection_function);
        }
        for (idx_t column_id = 0; column_id < output_schema_.size(); column_id++) {
            if (projections_[column_id] == nullptr) {
                projections_[column_id] = std::make_unique<UnitProjection>(output_schema_[column_id]);
            }
        }
    } else {
        projections_ = std::move(projections);
        for (auto &projection_function : projections_) {
            output_schema_.push_back(projection_function->output_name_);
        }
    }
}

ProjectionOperator::ProjectionOperator(const ExecutionContext &exec_ctx,
                                       const std::shared_ptr<Operator> &probe_child_operator,
                                       std::unique_ptr<Projection> &&projection,
                                       bool update_in_place)
    : ProjectionOperator(exec_ctx, probe_child_operator, TransToVec(std::move(projection)), update_in_place) {}

OperatorState ProjectionOperator::Next(Chunk &output_chunk) {
    // Reuse input chunk buffer
    Chunk& input_chunk = reusable_input_chunk_;
    input_chunk.clear();
    
    auto result = child_operators_[0]->Next(input_chunk);
    
    // Move data from input to output (avoid copy)
    output_chunk.clear();
    output_chunk.reserve(input_chunk.size());
    
    for (auto &data : input_chunk) {
        auto &old_tuple = data.first;
        Tuple new_tuple;
        new_tuple.reserve(projections_.size());
        
        for (auto &projection_function : projections_) {
            new_tuple.push_back(projection_function->Calc(old_tuple));
        }
        output_chunk.emplace_back(std::move(new_tuple), data.second);
    }
    
    return result;
}

void ProjectionOperator::SelfInit() {
    auto &input_schema = child_operators_[0]->GetOutputSchema();
    for (auto &projection_function : projections_) {
        projection_function->Init(input_schema);
    }
}

void ProjectionOperator::SelfCheck() {
    SelfInit();
}

}