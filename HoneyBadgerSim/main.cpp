#include <iostream>
#include <memory>
#include <vector>

#include "Node.h"
#include "Network.h"

int main()
{
    constexpr int totalNodes = 4;
    constexpr int totalEpochs = 4;
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

    std::cout << "=== Transaction broadcast across the network ===\n\n";

    Transaction tx0a = nodes[0]->GenerateTransaction("tx from node 0 - A");
    nodes[0]->BroadcastTransaction(tx0a, totalNodes);
    Transaction tx0b = nodes[0]->GenerateTransaction("tx from node 0 - B");
    nodes[0]->BroadcastTransaction(tx0b, totalNodes);

    Transaction tx1a = nodes[1]->GenerateTransaction("tx from node 1 - A");
    nodes[1]->BroadcastTransaction(tx1a, totalNodes);

    Transaction tx2a = nodes[2]->GenerateTransaction("tx from node 2 - A");
    nodes[2]->BroadcastTransaction(tx2a, totalNodes);

    Transaction tx3a = nodes[3]->GenerateTransaction("tx from node 3 - A");
    nodes[3]->BroadcastTransaction(tx3a, totalNodes);
    Transaction tx3b = nodes[3]->GenerateTransaction("tx from node 3 - B");
    nodes[3]->BroadcastTransaction(tx3b, totalNodes);

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

    std::cout << "=== Simplified HoneyBadger-style parameters ===\n";
    std::cout << "Nodes: " << totalNodes << " | assumed f=1 | required common subset size=N-f=3\n\n";

    for (int epoch = 0; epoch < totalEpochs; ++epoch)
    {
        std::cout << "\n==================================================\n";
        std::cout << "=== EPOCH " << epoch << " ===\n";
        std::cout << "==================================================\n\n";

        for (auto& node : nodes)
        {
            node->SetByzantineBehavior(ByzantineBehavior::Honest);
        }

        if (epoch == 0)
        {
            nodes[3]->SetByzantineBehavior(ByzantineBehavior::EquivocatingProposal);
        }
        else if (epoch == 1)
        {
            nodes[2]->SetByzantineBehavior(ByzantineBehavior::InvalidProposal);
        }
        else if (epoch == 2)
        {
            nodes[1]->SetByzantineBehavior(ByzantineBehavior::SilentProposal);
        }

        for (auto& node : nodes)
        {
            node->StartEpoch(epoch);
            node->PrintEpochInfo();
        }

        std::cout << "\n=== Broadcast proposal batches (all nodes propose, no leader) ===\n\n";
        for (auto& node : nodes)
        {
            Proposal proposal = node->CreateProposal(maxTransactionsPerProposal);
            node->BroadcastProposalBatch(proposal, totalNodes);
        }

        std::cout << "\n=== Deliver PROPOSAL messages ===\n";
        network.DeliverAll();
        std::cout << "\n=== Process PROPOSAL messages ===\n\n";
        for (auto& node : nodes)
        {
            node->ProcessInbox();
            std::cout << '\n';
        }

        std::cout << "=== Deliver ECHO messages ===\n";
        network.DeliverAll();
        std::cout << "\n=== Process ECHO messages ===\n\n";
        for (auto& node : nodes)
        {
            node->ProcessInbox();
            std::cout << '\n';
        }

        std::cout << "=== RBC-delivered proposals in epoch " << epoch << " ===\n\n";
        for (const auto& node : nodes)
        {
            node->PrintReceivedProposals();
            std::cout << '\n';
        }

        std::cout << "=== Blockchain after epoch " << epoch << " ===\n\n";
        for (const auto& node : nodes)
        {
            node->PrintBlockchain();
            std::cout << '\n';
        }

        std::cout << "=== Remaining transaction pools after epoch " << epoch << " ===\n\n";
        for (const auto& node : nodes)
        {
            node->PrintTransactionPool();
            std::cout << '\n';
        }
    }

    std::cout << "\n==================================================\n";
    std::cout << "=== FINAL BLOCKCHAINS ===\n";
    std::cout << "==================================================\n\n";

    for (const auto& node : nodes)
    {
        node->PrintBlockchain();
        std::cout << '\n';
    }

    return 0;
}
