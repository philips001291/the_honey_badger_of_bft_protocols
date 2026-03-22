#pragma once

#include <vector>
#include "Transaction.h"

struct Proposal
{
    int proposerId;
    std::vector<Transaction> transactions;
};
