#include "Node.h"
#include "Network.h"

#include <iostream>

int Node::sNextTransactionId = 1;

Node::Node(int id)
    : mId(id), mCurrentEpoch(0), mNetwork(nullptr)
{
}

void Node::AttachNetwork(Network* network)
{
    mNetwork = network;
}

int Node::GetId() const
{
    return mId;
}

void Node::SendMessage(int receiverId, MessageType type, const std::string& payload, int epoch)
{
    if (mNetwork == nullptr)
    {
        std::cerr << "Node " << mId << " has no network attached.\n";
        return;
    }

    Message msg{ mId, receiverId, type, payload, epoch };
    mNetwork->Send(msg);
}

void Node::Broadcast(MessageType type, const std::string& payload, int epoch, int totalNodes)
{
    for (int i = 0; i < totalNodes; ++i)
    {
        if (i == mId)
        {
            continue;
        }

        SendMessage(i, type, payload, epoch);
    }
}

void Node::ReceiveMessage(const Message& message)
{
    mInbox.push_back(message);
}

void Node::ProcessInbox()
{
    std::cout << "Node " << mId << " processing inbox:\n";

    if (mInbox.empty())
    {
        std::cout << "  [empty]\n";
        return;
    }

    for (const auto& msg : mInbox)
    {
        std::cout
            << "  Received from Node " << msg.senderId
            << " | epoch=" << msg.epoch
            << " | payload=\"" << msg.payload << "\"\n";
    }

    mInbox.clear();
}

void Node::AddTransaction(const Transaction& tx)
{
    mTransactionPool.push_back(tx);
}

void Node::GenerateTransaction(const std::string& payload)
{
    Transaction tx;
    tx.id = sNextTransactionId++;
    tx.creatorNodeId = mId;
    tx.payload = payload;

    mTransactionPool.push_back(tx);
}

void Node::PrintTransactionPool() const
{
    std::cout << "Node " << mId << " transaction pool:\n";

    if (mTransactionPool.empty())
    {
        std::cout << "  [empty]\n";
        return;
    }

    for (const auto& tx : mTransactionPool)
    {
        std::cout
            << "  tx.id=" << tx.id
            << " | creator=" << tx.creatorNodeId
            << " | payload=\"" << tx.payload << "\"\n";
    }
}