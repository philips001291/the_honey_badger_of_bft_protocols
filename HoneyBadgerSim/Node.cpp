#include "Node.h"
#include "Network.h"

#include <algorithm>
#include <iostream>
#include <sstream>
#include <stdexcept>

int Node::sNextTransactionId = 1;

Node::Node(int id)
    : mId(id),
    mCurrentEpoch(0),
    mTotalNodes(0),
    mNetwork(nullptr)
{
}

void Node::AttachNetwork(Network* network)
{
    mNetwork = network;
}

void Node::SetTotalNodes(int totalNodes)
{
    mTotalNodes = totalNodes;
}

int Node::GetId() const
{
    return mId;
}

int Node::GetCurrentEpoch() const
{
    return mCurrentEpoch;
}

bool Node::IsLeaderForCurrentEpoch() const
{
    return mId == GetLeaderForEpoch(mCurrentEpoch);
}

void Node::StartEpoch(int epoch)
{
    mCurrentEpoch = epoch;
    mReceivedProposals.clear();
    mVotesByProposal.clear();
    mCommittedProposals.clear();
    mCommitBroadcasted.clear();
}

void Node::PrintEpochInfo() const
{
    std::cout
        << "Node " << mId
        << " | epoch=" << mCurrentEpoch
        << " | leader=" << GetLeaderForEpoch(mCurrentEpoch);

    if (IsLeaderForCurrentEpoch())
    {
        std::cout << " -> I am leader";
    }

    std::cout << '\n';
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
        if (msg.epoch != mCurrentEpoch)
        {
            std::cout
                << "  Ignoring message from Node " << msg.senderId
                << " | message epoch=" << msg.epoch
                << " | current epoch=" << mCurrentEpoch
                << " -> stale/future message\n";
            continue;
        }

        if (msg.type == MessageType::TransactionBroadcast)
        {
            Transaction tx = DeserializeTransaction(msg.payload);

            std::cout
                << "  Received transaction from Node " << msg.senderId
                << " | tx.id=" << tx.id
                << " | creator=" << tx.creatorNodeId
                << " | payload=\"" << tx.payload << "\"";

            if (!HasTransaction(tx.id))
            {
                AddTransaction(tx);
                std::cout << " -> added to local pool\n";
            }
            else
            {
                std::cout << " -> duplicate ignored\n";
            }
        }
        else if (msg.type == MessageType::ProposalBroadcast)
        {
            Proposal proposal = DeserializeProposal(msg.payload);
            const int expectedLeader = GetLeaderForEpoch(mCurrentEpoch);

            std::cout
                << "  Received proposal from Node " << msg.senderId
                << " | proposer=" << proposal.proposerId
                << " | expected leader=" << expectedLeader
                << " | tx_count=" << proposal.transactions.size();

            if (proposal.proposerId != expectedLeader || msg.senderId != expectedLeader)
            {
                std::cout << " -> rejected (not from current leader)\n";
                continue;
            }

            if (!HasProposalFrom(proposal.proposerId))
            {
                mReceivedProposals.push_back(proposal);
                std::cout << " -> stored";

                if (IsProposalValid(proposal))
                {
                    SendMessage(
                        proposal.proposerId,
                        MessageType::Vote,
                        std::to_string(proposal.proposerId),
                        mCurrentEpoch);

                    std::cout << " -> vote sent to leader";
                }
                else
                {
                    std::cout << " -> invalid proposal, no vote";
                }

                std::cout << '\n';
            }
            else
            {
                std::cout << " -> duplicate ignored\n";
            }
        }
        else if (msg.type == MessageType::Vote)
        {
            int proposerId = std::stoi(msg.payload);

            std::cout
                << "  Received vote from Node " << msg.senderId
                << " for proposal of Node " << proposerId;

            RegisterVote(proposerId, msg.senderId);

            std::cout
                << " | unique votes=" << mVotesByProposal[proposerId].size()
                << '\n';

            if (proposerId == mId && IsLeaderForCurrentEpoch())
            {
                TryCommitProposal(proposerId, mTotalNodes);
            }
        }
        else if (msg.type == MessageType::Commit)
        {
            int proposerId = std::stoi(msg.payload);
            const int expectedLeader = GetLeaderForEpoch(mCurrentEpoch);

            std::cout
                << "  Received commit for proposal of Node "
                << proposerId;

            if (proposerId != expectedLeader || msg.senderId != expectedLeader)
            {
                std::cout << " -> rejected (commit not from current leader)\n";
                continue;
            }

            if (!HasCommittedProposal(proposerId))
            {
                mCommittedProposals.insert(proposerId);
                std::cout << " -> committed\n";
            }
            else
            {
                std::cout << " -> duplicate ignored\n";
            }
        }
        else
        {
            std::cout
                << "  Received from Node " << msg.senderId
                << " | epoch=" << msg.epoch
                << " | payload=\"" << msg.payload << "\"\n";
        }
    }

    mInbox.clear();
}

void Node::AddTransaction(const Transaction& tx)
{
    if (!HasTransaction(tx.id))
    {
        mTransactionPool.push_back(tx);
    }
}

Transaction Node::GenerateTransaction(const std::string& payload)
{
    Transaction tx;
    tx.id = sNextTransactionId++;
    tx.creatorNodeId = mId;
    tx.payload = payload;

    mTransactionPool.push_back(tx);
    return tx;
}

void Node::BroadcastTransaction(const Transaction& tx, int totalNodes)
{
    Broadcast(MessageType::TransactionBroadcast, SerializeTransaction(tx), mCurrentEpoch, totalNodes);
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

Proposal Node::CreateProposal(int maxTransactions) const
{
    Proposal proposal;
    proposal.proposerId = mId;

    const int txCount = std::min<int>(maxTransactions, static_cast<int>(mTransactionPool.size()));
    for (int i = 0; i < txCount; ++i)
    {
        proposal.transactions.push_back(mTransactionPool[i]);
    }

    return proposal;
}

void Node::BroadcastProposal(const Proposal& proposal, int totalNodes)
{
    Broadcast(MessageType::ProposalBroadcast, SerializeProposal(proposal), mCurrentEpoch, totalNodes);
}

void Node::PrintReceivedProposals() const
{
    std::cout << "Node " << mId << " received proposals:\n";

    if (mReceivedProposals.empty())
    {
        std::cout << "  [empty]\n";
        return;
    }

    for (const auto& proposal : mReceivedProposals)
    {
        std::cout << "  Proposal from Node " << proposal.proposerId << ":\n";

        if (proposal.transactions.empty())
        {
            std::cout << "    [empty]\n";
            continue;
        }

        for (const auto& tx : proposal.transactions)
        {
            std::cout
                << "    tx.id=" << tx.id
                << " | creator=" << tx.creatorNodeId
                << " | payload=\"" << tx.payload << "\"\n";
        }
    }
}

bool Node::HasTransaction(int txId) const
{
    for (const auto& tx : mTransactionPool)
    {
        if (tx.id == txId)
        {
            return true;
        }
    }

    return false;
}

bool Node::HasProposalFrom(int proposerId) const
{
    for (const auto& proposal : mReceivedProposals)
    {
        if (proposal.proposerId == proposerId)
        {
            return true;
        }
    }

    return false;
}

bool Node::IsProposalValid(const Proposal& proposal) const
{
    for (const auto& tx : proposal.transactions)
    {
        if (!HasTransaction(tx.id))
        {
            return false;
        }
    }

    return true;
}

bool Node::HasCommittedProposal(int proposerId) const
{
    return mCommittedProposals.find(proposerId) != mCommittedProposals.end();
}

bool Node::HasBroadcastCommitFor(int proposerId) const
{
    return mCommitBroadcasted.find(proposerId) != mCommitBroadcasted.end();
}

int Node::GetRequiredVotes() const
{
    if (mTotalNodes <= 1)
    {
        return 1;
    }

    return (mTotalNodes / 2) + 1;
}

int Node::GetLeaderForEpoch(int epoch) const
{
    if (mTotalNodes <= 0)
    {
        return 0;
    }

    return epoch % mTotalNodes;
}

void Node::RegisterVote(int proposerId, int voterId)
{
    mVotesByProposal[proposerId].insert(voterId);
}

void Node::TryCommitProposal(int proposerId, int totalNodes)
{
    const int currentVotes = static_cast<int>(mVotesByProposal[proposerId].size());
    const int requiredVotes = GetRequiredVotes();

    if (currentVotes >= requiredVotes && !HasBroadcastCommitFor(proposerId))
    {
        std::cout
            << "  Node " << mId
            << " collected enough votes for leader proposal"
            << " | votes=" << currentVotes
            << "/" << requiredVotes
            << " -> broadcasting COMMIT\n";

        mCommitBroadcasted.insert(proposerId);
        mCommittedProposals.insert(proposerId);

        Broadcast(
            MessageType::Commit,
            std::to_string(proposerId),
            mCurrentEpoch,
            totalNodes);
    }
}

std::string Node::SerializeTransaction(const Transaction& tx) const
{
    return std::to_string(tx.id) + "|" + std::to_string(tx.creatorNodeId) + "|" + tx.payload;
}

Transaction Node::DeserializeTransaction(const std::string& data) const
{
    const std::size_t firstSep = data.find('|');
    const std::size_t secondSep = data.find('|', firstSep + 1);

    if (firstSep == std::string::npos || secondSep == std::string::npos)
    {
        throw std::runtime_error("Invalid transaction payload format.");
    }

    Transaction tx;
    tx.id = std::stoi(data.substr(0, firstSep));
    tx.creatorNodeId = std::stoi(data.substr(firstSep + 1, secondSep - firstSep - 1));
    tx.payload = data.substr(secondSep + 1);
    return tx;
}

std::string Node::SerializeProposal(const Proposal& proposal) const
{
    std::ostringstream oss;
    oss << proposal.proposerId;

    for (const auto& tx : proposal.transactions)
    {
        oss << '\n' << SerializeTransaction(tx);
    }

    return oss.str();
}

Proposal Node::DeserializeProposal(const std::string& data) const
{
    std::istringstream iss(data);
    std::string line;
    Proposal proposal{};

    if (!std::getline(iss, line))
    {
        throw std::runtime_error("Invalid proposal payload format.");
    }

    proposal.proposerId = std::stoi(line);

    while (std::getline(iss, line))
    {
        if (!line.empty())
        {
            proposal.transactions.push_back(DeserializeTransaction(line));
        }
    }

    return proposal;
}