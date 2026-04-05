#pragma once

#include <unordered_map>
#include <vector>
#include "Message.h"

class Node;

class Network
{
private:
    std::unordered_map<int, Node*> mNodes;
    std::vector<Message> mPendingMessages;
public:
    void RegisterNode(Node* node);
    void Send(const Message& message);
    void DeliverAll();
    bool HasPendingMessages() const;
    std::size_t PendingCount() const;
};
