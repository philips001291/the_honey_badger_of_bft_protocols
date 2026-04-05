#pragma once

#include <string>

enum class MessageType
{
    ProposalBroadcast,
    Echo,
    Debug,
    TransactionBroadcast
};

struct Message
{
    int senderId;
    int receiverId;
    MessageType type;
    std::string payload;
    int epoch;
};
