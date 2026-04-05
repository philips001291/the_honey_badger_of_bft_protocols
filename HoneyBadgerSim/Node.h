#pragma once

#include <string>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "Block.h"
#include "Message.h"
#include "Proposal.h"
#include "Transaction.h"

class Network;

enum class ByzantineBehavior
{
    Honest,
    SilentProposal,
    InvalidProposal,
    EquivocatingProposal
};

class Node
{
private:
    int mId;
    int mCurrentEpoch;
    int mTotalNodes;
    Network* mNetwork;
    ByzantineBehavior mBehavior;

    std::vector<Message> mInbox;
    std::vector<Transaction> mTransactionPool;
    std::vector<Block> mBlockchain;

    std::unordered_map<int, Proposal> mDirectProposalsBySender;
    std::unordered_map<int, Proposal> mRbcDeliveredProposals;
    std::unordered_map<int, std::unordered_map<std::string, std::unordered_set<int>>> mEchoesByProposer;
    std::unordered_set<int> mEchoBroadcastedForProposer;
    bool mEpochFinalized;

    static int sNextTransactionId;

    bool HasTransaction(int txId) const;
    bool HasBlockForEpoch(int epoch) const;
    bool IsProposalValid(const Proposal& proposal) const;

    int GetMaxFaultyNodes() const;
    int GetRequiredProposalCount() const;

    void RegisterEcho(int proposerId, const Proposal& proposal, int echoSenderId);
    void TryDeliverRbcProposal(int proposerId, const Proposal& proposal);
    void TryFinalizeCommonSubset();
    void FinalizeEpochBlock();
    void RemoveCommittedTransactionsFromPool(const std::vector<Transaction>& committedTransactions);

    Proposal BuildInvalidProposalFrom(const Proposal& baseProposal) const;
    Proposal BuildEquivocatingProposalForReceiver(const Proposal& baseProposal, int receiverId) const;

    void BroadcastProposalHonest(const Proposal& proposal, int totalNodes);
    void BroadcastProposalEquivocating(const Proposal& proposal, int totalNodes);
    void BroadcastEchoForProposal(const Proposal& proposal, int totalNodes);

    std::string SerializeTransaction(const Transaction& tx) const;
    Transaction DeserializeTransaction(const std::string& data) const;

    std::string SerializeProposal(const Proposal& proposal) const;
    Proposal DeserializeProposal(const std::string& data) const;

    std::string SerializeEchoPayload(int proposerId, const Proposal& proposal) const;
    Proposal DeserializeEchoPayload(const std::string& data, int& proposerId) const;

public:
    Node(int id);

    void AttachNetwork(Network* network);
    void SetTotalNodes(int totalNodes);
    void SetByzantineBehavior(ByzantineBehavior behavior);

    int GetId() const;
    int GetCurrentEpoch() const;
    ByzantineBehavior GetByzantineBehavior() const;
    std::string GetBehaviorName() const;

    void StartEpoch(int epoch);
    void PrintEpochInfo() const;

    void SendMessage(int receiverId, MessageType type, const std::string& payload, int epoch);
    void Broadcast(MessageType type, const std::string& payload, int epoch, int totalNodes);

    void ReceiveMessage(const Message& message);
    void ProcessInbox();
    void TickTimeouts();

    void AddTransaction(const Transaction& tx);
    Transaction GenerateTransaction(const std::string& payload);
    void BroadcastTransaction(const Transaction& tx, int totalNodes);
    void PrintTransactionPool() const;

    Proposal CreateProposal(int maxTransactions) const;
    void BroadcastProposalBatch(const Proposal& proposal, int totalNodes);
    void PrintReceivedProposals() const;
    void PrintBlockchain() const;
    void PrintState() const;
    void PrintPbftLogSummary() const;
};
