#pragma once

#include "execution/expression/filter.hpp"
#include "execution/operator.hpp"

namespace babydb {

class FilterOperator : public Operator {
public:
    FilterOperator(const ExecutionContext &exec_ctx,
                   const std::shared_ptr<Operator> &probe_child_operator,
                   std::unique_ptr<Filter> &&filter);

    FilterOperator(const ExecutionContext &exec_ctx,
                   const std::shared_ptr<Operator> &probe_child_operator,
                   std::vector<std::unique_ptr<Filter>> &&filters);

    ~FilterOperator() = default;

    OperatorState Next(Chunk &output_chunk) override;

    std::string BindTableName() override { return child_operators_[0]->BindTableName(); }
    
    void Reset() override {
        SelfInit();
        child_operators_[0]->Reset();
    }

private:
    void SelfInit() override;
    void SelfCheck() override;

private:
    std::vector<std::unique_ptr<Filter>> filters_;
};

}