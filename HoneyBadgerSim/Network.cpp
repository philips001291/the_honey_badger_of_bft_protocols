#include "Network.h"
#include "Node.h"

#include <iostream>
#include <algorithm>
#include <random>

void Network::RegisterNode(Node* node)
{
    if (node == nullptr)
    {
        return;
    }

    mNodes[node->GetId()] = node;
}

void Network::Send(const Message& message)
{
    std::cout
        << "Network queued message: "
        << "from " << message.senderId
        << " to " << message.receiverId
        << " | epoch=" << message.epoch
        << " | payload=\"" << message.payload << "\"\n";

    mPendingMessages.push_back(message);
}

void Network::DeliverAll()
{
    std::cout << "\nNetwork delivering messages...\n";

    std::random_device rd;
    std::mt19937 g(rd());

    std::shuffle(mPendingMessages.begin(), mPendingMessages.end(), g);

    for (const auto& msg : mPendingMessages)
    {
        auto it = mNodes.find(msg.receiverId);
        if (it != mNodes.end() && it->second != nullptr)
        {
            it->second->ReceiveMessage(msg);
        }
    }

    mPendingMessages.clear();
}