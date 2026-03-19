#include <iostream>
#include <vector>
#include <memory>

#include "Node.h"
#include "Network.h"

int main()
{
   /*  constexpr int totalNodes = 44;

    Network network;
    std::vector<std::unique_ptr<Node>> nodes;

    for (int i = 0; i < totalNodes; ++i)
    {
        nodes.push_back(std::make_unique<Node>(i));
        nodes.back()->AttachNetwork(&network);
        network.RegisterNode(nodes.back().get());
    }

    std::cout << "=== Step 1: Basic network simulation ===\n\n";

    nodes[0]->Broadcast(MessageType::Debug, "Hello from Node 0", 0, totalNodes);

    network.DeliverAll();

    std::cout << "\n=== Processing inboxes ===\n\n";
    for (auto& node : nodes)
    {
        node->ProcessInbox();
    }*/

    Node node0(0);
    Node node1(1);
    Node node2(2);

    node0.GenerateTransaction("tx from node 0 - A");
    node0.GenerateTransaction("tx from node 0 - B");

    node1.GenerateTransaction("tx from node 1 - A");

    node2.GenerateTransaction("tx from node 2 - A");
    node2.GenerateTransaction("tx from node 2 - B");

    node0.PrintTransactionPool();
    node1.PrintTransactionPool();
    node2.PrintTransactionPool();

    return 0;
}