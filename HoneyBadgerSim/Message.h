#pragma once

#include <string>

enum class MessageType
{
    Proposal,
    Vote,
    Commit,
    Debug,
    TransactionBroadcast,
    ProposalBroadcast
};

struct Message
{
    int senderId;
    int receiverId;
    MessageType type;
    std::string payload;
    int epoch;
};
