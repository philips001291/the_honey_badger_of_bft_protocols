#pragma once

#include <vector>
#include <string>
#include <unordered_map>
#include <unordered_set>

#include "Message.h"
#include "Transaction.h"
#include "Proposal.h"

class Network;

class Node
{
private:
    int mId;
    int mCurrentEpoch;
    int mTotalNodes;
    Network* mNetwork;
    std::vector<Message> mInbox;
    std::vector<Transaction> mTransactionPool;
    std::vector<Proposal> mReceivedProposals;

    std::unordered_map<int, std::unordered_set<int>> mVotesByProposal;
    std::unordered_set<int> mCommittedProposals;
    std::unordered_set<int> mCommitBroadcasted;

    static int sNextTransactionId;

    bool HasTransaction(int txId) const;
    bool HasProposalFrom(int proposerId) const;
    bool IsProposalValid(const Proposal& proposal) const;
    bool HasCommittedProposal(int proposerId) const;
    bool HasBroadcastCommitFor(int proposerId) const;
    int GetRequiredVotes() const;
    int GetLeaderForEpoch(int epoch) const;

    void RegisterVote(int proposerId, int voterId);
    void TryCommitProposal(int proposerId, int totalNodes);

    std::string SerializeTransaction(const Transaction& tx) const;
    Transaction DeserializeTransaction(const std::string& data) const;

    std::string SerializeProposal(const Proposal& proposal) const;
    Proposal DeserializeProposal(const std::string& data) const;

public:
    Node(int id);

    void AttachNetwork(Network* network);
    void SetTotalNodes(int totalNodes);

    int GetId() const;
    int GetCurrentEpoch() const;
    bool IsLeaderForCurrentEpoch() const;

    void StartEpoch(int epoch);
    void PrintEpochInfo() const;

    void SendMessage(int receiverId, MessageType type, const std::string& payload, int epoch);
    void Broadcast(MessageType type, const std::string& payload, int epoch, int totalNodes);

    void ReceiveMessage(const Message& message);
    void ProcessInbox();

    void AddTransaction(const Transaction& tx);
    Transaction GenerateTransaction(const std::string& payload);
    void BroadcastTransaction(const Transaction& tx, int totalNodes);
    void PrintTransactionPool() const;

    Proposal CreateProposal(int maxTransactions) const;
    void BroadcastProposal(const Proposal& proposal, int totalNodes);
    void PrintReceivedProposals() const;
};