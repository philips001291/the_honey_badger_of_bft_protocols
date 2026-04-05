#pragma once

#include <vector>
#include "Transaction.h"

struct Block
{
    int epoch{};
    int proposerId{};
    std::vector<Transaction> transactions;
};
