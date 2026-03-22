#include <iostream>
#include <memory>
#include <vector>

#include "Node.h"
#include "Network.h"

int main()
{
    constexpr int totalNodes = 3;
    constexpr int totalEpochs = 3;
    constexpr int maxTransactionsPerProposal = 2;

    Network network;
    std::vector<std::unique_ptr<Node>> nodes;

    for (int i = 0; i < totalNodes; ++i)
    {
        nodes.push_back(std::make_unique<Node>(i));
        nodes.back()->AttachNetwork(&network);
        nodes.back()->SetTotalNodes(totalNodes);
        network.RegisterNode(nodes.back().get());
    }

    std::cout << "=== Step 1: Transaction broadcast across the network ===\n\n";

    Transaction tx0a = nodes[0]->GenerateTransaction("tx from node 0 - A");
    nodes[0]->BroadcastTransaction(tx0a, totalNodes);

    Transaction tx0b = nodes[0]->GenerateTransaction("tx from node 0 - B");
    nodes[0]->BroadcastTransaction(tx0b, totalNodes);

    Transaction tx1a = nodes[1]->GenerateTransaction("tx from node 1 - A");
    nodes[1]->BroadcastTransaction(tx1a, totalNodes);

    Transaction tx2a = nodes[2]->GenerateTransaction("tx from node 2 - A");
    nodes[2]->BroadcastTransaction(tx2a, totalNodes);

    Transaction tx2b = nodes[2]->GenerateTransaction("tx from node 2 - B");
    nodes[2]->BroadcastTransaction(tx2b, totalNodes);

    network.DeliverAll();

    std::cout << "\n=== Processing inboxes after transaction broadcast ===\n\n";
    for (auto& node : nodes)
    {
        node->ProcessInbox();
        std::cout << '\n';
    }

    std::cout << "=== Transaction pools before consensus ===\n\n";
    for (const auto& node : nodes)
    {
        node->PrintTransactionPool();
        std::cout << '\n';
    }

    for (int epoch = 0; epoch < totalEpochs; ++epoch)
    {
        std::cout << "\n==================================================\n";
        std::cout << "=== EPOCH " << epoch << " ===\n";
        std::cout << "==================================================\n\n";

        for (auto& node : nodes)
        {
            node->StartEpoch(epoch);
            node->PrintEpochInfo();
        }

        std::cout << '\n';

        const int leaderId = epoch % totalNodes;

        std::cout
            << "Leader for epoch " << epoch
            << " is Node " << leaderId << "\n\n";

        Proposal leaderProposal = nodes[leaderId]->CreateProposal(maxTransactionsPerProposal);

        std::cout
            << "Node " << leaderId
            << " broadcasts proposal for epoch " << epoch << "\n";

        nodes[leaderId]->BroadcastProposal(leaderProposal, totalNodes);

        network.DeliverAll();

        std::cout << "\n=== Processing inboxes after proposal broadcast ===\n\n";
        for (auto& node : nodes)
        {
            node->ProcessInbox();
            std::cout << '\n';
        }

        std::cout << "=== Deliver votes ===\n\n";
        network.DeliverAll();

        std::cout << "\n=== Processing inboxes after vote delivery ===\n\n";
        for (auto& node : nodes)
        {
            node->ProcessInbox();
            std::cout << '\n';
        }

        std::cout << "=== Deliver commits ===\n\n";
        network.DeliverAll();

        std::cout << "\n=== Processing inboxes after commit delivery ===\n\n";
        for (auto& node : nodes)
        {
            node->ProcessInbox();
            std::cout << '\n';
        }

        std::cout << "=== Proposals stored in epoch " << epoch << " ===\n\n";
        for (const auto& node : nodes)
        {
            node->PrintReceivedProposals();
            std::cout << '\n';
        }
    }

    return 0;
}