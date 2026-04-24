#pragma once

#include "execution/operator.hpp"

namespace babydb {

class ValueOperator : public Operator {
public:
    ValueOperator(const ExecutionContext &execute_context, const Schema &output_schema, std::vector<Tuple> &&tuples);
    ~ValueOperator() override = default;

    void SelfInit() override;
    void SelfCheck() override;
    OperatorState Next(Chunk &output_chunk) override;

private:
    std::vector<SharedTuple> shared_tuples_;
    idx_t next_tuple_id{0};
};

}