#pragma once
#include <string>

struct Transaction
{
    int id;
    int creatorNodeId;
    std::string payload;
};