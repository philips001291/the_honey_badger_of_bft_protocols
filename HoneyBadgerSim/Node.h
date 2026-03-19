#pragma once

#include <vector>
#include <string>
#include "Message.h"
#include "Transaction.h"

class Network;

class Node
{
private:
    int mId;
    int mCurrentEpoch;
    Network* mNetwork;
    std::vector<Message> mInbox;
    std::vector<Transaction> mTransactionPool;
    static int sNextTransactionId;

public:
    Node(int id);

    void AttachNetwork(Network* network);

    int GetId() const;

    void SendMessage(int receiverId, MessageType type, const std::string& payload, int epoch);
    void Broadcast(MessageType type, const std::string& payload, int epoch, int totalNodes);

    void ReceiveMessage(const Message& message);
    void ProcessInbox();

    void AddTransaction(const Transaction& tx);
    void GenerateTransaction(const std::string& payload);
    void PrintTransactionPool() const;
};