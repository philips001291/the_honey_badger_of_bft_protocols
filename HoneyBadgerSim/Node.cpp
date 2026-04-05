#include "Node.h"
#include "Network.h"

#include <algorithm>
#include <iostream>
#include <set>
#include <sstream>
#include <stdexcept>

int Node::sNextTransactionId = 1;

Node::Node(int id)
    : mId(id),
    mCurrentEpoch(0),
    mTotalNodes(0),
    mNetwork(nullptr),
    mBehavior(ByzantineBehavior::Honest),
    mEpochFinalized(false)
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

void Node::SetByzantineBehavior(ByzantineBehavior behavior)
{
    mBehavior = behavior;
}

int Node::GetId() const
{
    return mId;
}

int Node::GetCurrentEpoch() const
{
    return mCurrentEpoch;
}

ByzantineBehavior Node::GetByzantineBehavior() const
{
    return mBehavior;
}

std::string Node::GetBehaviorName() const
{
    switch (mBehavior)
    {
    case ByzantineBehavior::Honest:
        return "Honest";
    case ByzantineBehavior::SilentProposal:
        return "SilentProposal";
    case ByzantineBehavior::InvalidProposal:
        return "InvalidProposal";
    case ByzantineBehavior::EquivocatingProposal:
        return "EquivocatingProposal";
    }

    return "Unknown";
}

void Node::StartEpoch(int epoch)
{
    mCurrentEpoch = epoch;
    mInbox.clear();
    mDirectProposalsBySender.clear();
    mRbcDeliveredProposals.clear();
    mEchoesByProposer.clear();
    mEchoBroadcastedForProposer.clear();
    mEpochFinalized = false;
}

void Node::PrintEpochInfo() const
{
    std::cout
        << "Node " << mId
        << " | epoch=" << mCurrentEpoch
        << " | behavior=" << GetBehaviorName() << '\n';
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

            std::cout
                << "  Received PROPOSAL from Node " << msg.senderId
                << " | proposer=" << proposal.proposerId
                << " | tx_count=" << proposal.transactions.size();

            if (proposal.proposerId != msg.senderId)
            {
                std::cout << " -> rejected (sender/proposer mismatch)\n";
                continue;
            }

            mDirectProposalsBySender[msg.senderId] = proposal;

            if (!IsProposalValid(proposal))
            {
                std::cout << " -> invalid, no ECHO\n";
                continue;
            }

            std::cout << " -> valid";

            if (mEchoBroadcastedForProposer.find(proposal.proposerId) == mEchoBroadcastedForProposer.end())
            {
                std::cout << " -> broadcasting ECHO\n";
                BroadcastEchoForProposal(proposal, mTotalNodes);
            }
            else
            {
                std::cout << " -> ECHO already sent\n";
            }
        }
        else if (msg.type == MessageType::Echo)
        {
            int proposerId = -1;
            Proposal proposal = DeserializeEchoPayload(msg.payload, proposerId);

            std::cout
                << "  Received ECHO from Node " << msg.senderId
                << " for proposer " << proposerId;

            if (proposal.proposerId != proposerId)
            {
                std::cout << " -> rejected (echo payload mismatch)\n";
                continue;
            }

            if (!IsProposalValid(proposal))
            {
                std::cout << " -> rejected (echo contains invalid proposal)\n";
                continue;
            }

            RegisterEcho(proposerId, proposal, msg.senderId);
            const std::string serializedProposal = SerializeProposal(proposal);
            const int echoCount = static_cast<int>(mEchoesByProposer[proposerId][serializedProposal].size());

            std::cout
                << " | matching echoes=" << echoCount
                << '/' << GetRequiredProposalCount() << '\n';

            TryDeliverRbcProposal(proposerId, proposal);
            TryFinalizeCommonSubset();
        }
        else
        {
            std::cout
                << "  Received legacy/unsupported message from Node " << msg.senderId
                << " | epoch=" << msg.epoch
                << " | payload=\"" << msg.payload << "\"\n";
        }
    }

    mInbox.clear();
}

void Node::TickTimeouts()
{
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

void Node::BroadcastProposalBatch(const Proposal& proposal, int totalNodes)
{
    if (mBehavior == ByzantineBehavior::SilentProposal)
    {
        std::cout << "Node " << mId << " is Byzantine: stays silent and sends no proposal\n";
        return;
    }

    Proposal outgoingProposal = proposal;

    if (mBehavior == ByzantineBehavior::InvalidProposal)
    {
        outgoingProposal = BuildInvalidProposalFrom(proposal);
        std::cout << "Node " << mId << " is Byzantine: broadcasting INVALID proposal\n";
        BroadcastProposalHonest(outgoingProposal, totalNodes);
        return;
    }

    if (mBehavior == ByzantineBehavior::EquivocatingProposal)
    {
        std::cout << "Node " << mId << " is Byzantine: broadcasting DIFFERENT proposals to different nodes\n";
        BroadcastProposalEquivocating(proposal, totalNodes);
        return;
    }

    std::cout << "Node " << mId << " broadcasts proposal batch\n";
    BroadcastProposalHonest(outgoingProposal, totalNodes);
    BroadcastEchoForProposal(outgoingProposal, totalNodes);
}

void Node::PrintReceivedProposals() const
{
    std::cout << "Node " << mId << " RBC-delivered proposals:\n";

    if (mRbcDeliveredProposals.empty())
    {
        std::cout << "  [empty]\n";
        return;
    }

    for (const auto& [proposerId, proposal] : mRbcDeliveredProposals)
    {
        std::cout << "  Proposal from Node " << proposerId << ":\n";

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

void Node::PrintBlockchain() const
{
    std::cout << "Node " << mId << " blockchain:\n";

    if (mBlockchain.empty())
    {
        std::cout << "  [empty]\n";
        return;
    }

    for (const auto& block : mBlockchain)
    {
        std::cout
            << "  Block epoch=" << block.epoch
            << " | proposer=" << block.proposerId
            << " | tx_count=" << block.transactions.size() << '\n';

        if (block.transactions.empty())
        {
            std::cout << "    [empty]\n";
            continue;
        }

        for (const auto& tx : block.transactions)
        {
            std::cout
                << "    tx.id=" << tx.id
                << " | creator=" << tx.creatorNodeId
                << " | payload=\"" << tx.payload << "\"\n";
        }
    }
}

void Node::PrintState() const
{
    std::cout << "Node " << mId << " state:\n";
    if (mBlockchain.empty())
    {
        std::cout << "  [state is implicit through committed blocks]\n";
    }
    else
    {
        std::cout << "  committed blocks=" << mBlockchain.size() << '\n';
    }
}

void Node::PrintPbftLogSummary() const
{
    std::cout << "Node " << mId << " HoneyBadger/RBC summary:\n";
    std::cout << "  epoch=" << mCurrentEpoch << " | directProposals=" << mDirectProposalsBySender.size()
        << " | delivered=" << mRbcDeliveredProposals.size()
        << " | finalized=" << (mEpochFinalized ? "yes" : "no") << '\n';
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

bool Node::HasBlockForEpoch(int epoch) const
{
    for (const auto& block : mBlockchain)
    {
        if (block.epoch == epoch)
        {
            return true;
        }
    }

    return false;
}

bool Node::IsProposalValid(const Proposal& proposal) const
{
    if (proposal.transactions.empty())
    {
        return false;
    }

    for (const auto& tx : proposal.transactions)
    {
        if (!HasTransaction(tx.id))
        {
            return false;
        }
    }

    return true;
}

int Node::GetMaxFaultyNodes() const
{
    if (mTotalNodes < 4)
    {
        return 0;
    }

    return (mTotalNodes - 1) / 3;
}

int Node::GetRequiredProposalCount() const
{
    return mTotalNodes - GetMaxFaultyNodes();
}

void Node::RegisterEcho(int proposerId, const Proposal& proposal, int echoSenderId)
{
    const std::string serializedProposal = SerializeProposal(proposal);
    mEchoesByProposer[proposerId][serializedProposal].insert(echoSenderId);
}

void Node::TryDeliverRbcProposal(int proposerId, const Proposal& proposal)
{
    if (mRbcDeliveredProposals.find(proposerId) != mRbcDeliveredProposals.end())
    {
        return;
    }

    const std::string serializedProposal = SerializeProposal(proposal);
    const int echoCount = static_cast<int>(mEchoesByProposer[proposerId][serializedProposal].size());

    if (echoCount >= GetRequiredProposalCount())
    {
        std::cout
            << "  Node " << mId
            << " RBC-delivered proposal of Node " << proposerId
            << " with " << echoCount << " matching echoes\n";

        mRbcDeliveredProposals[proposerId] = proposal;
    }
}

void Node::TryFinalizeCommonSubset()
{
    if (mEpochFinalized)
    {
        return;
    }

    if (static_cast<int>(mRbcDeliveredProposals.size()) >= GetRequiredProposalCount())
    {
        std::cout
            << "  Node " << mId
            << " collected common subset of " << mRbcDeliveredProposals.size()
            << " proposals -> finalizing epoch block\n";

        FinalizeEpochBlock();
    }
}

void Node::FinalizeEpochBlock()
{
    if (HasBlockForEpoch(mCurrentEpoch))
    {
        mEpochFinalized = true;
        return;
    }

    std::set<int> seenTransactionIds;
    std::vector<Transaction> mergedTransactions;

    std::vector<int> proposers;
    proposers.reserve(mRbcDeliveredProposals.size());
    for (const auto& [proposerId, _] : mRbcDeliveredProposals)
    {
        proposers.push_back(proposerId);
    }
    std::sort(proposers.begin(), proposers.end());

    for (int proposerId : proposers)
    {
        const Proposal& proposal = mRbcDeliveredProposals.at(proposerId);
        for (const auto& tx : proposal.transactions)
        {
            if (seenTransactionIds.insert(tx.id).second)
            {
                mergedTransactions.push_back(tx);
            }
        }
    }

    std::sort(
        mergedTransactions.begin(),
        mergedTransactions.end(),
        [](const Transaction& left, const Transaction& right)
        {
            return left.id < right.id;
        });

    Block block;
    block.epoch = mCurrentEpoch;
    block.proposerId = -1;
    block.transactions = mergedTransactions;

    mBlockchain.push_back(block);
    RemoveCommittedTransactionsFromPool(mergedTransactions);
    mEpochFinalized = true;
}

void Node::RemoveCommittedTransactionsFromPool(const std::vector<Transaction>& committedTransactions)
{
    mTransactionPool.erase(
        std::remove_if(
            mTransactionPool.begin(),
            mTransactionPool.end(),
            [&](const Transaction& localTx)
            {
                for (const auto& committedTx : committedTransactions)
                {
                    if (localTx.id == committedTx.id)
                    {
                        return true;
                    }
                }

                return false;
            }),
        mTransactionPool.end());
}

Proposal Node::BuildInvalidProposalFrom(const Proposal& baseProposal) const
{
    Proposal invalid = baseProposal;

    if (!invalid.transactions.empty())
    {
        invalid.transactions[0].id += 10000;
        invalid.transactions[0].payload += " [CORRUPTED]";
    }

    return invalid;
}

Proposal Node::BuildEquivocatingProposalForReceiver(const Proposal& baseProposal, int receiverId) const
{
    Proposal altered = baseProposal;

    if (!altered.transactions.empty())
    {
        altered.transactions[0].payload += " [variant-for-node-" + std::to_string(receiverId) + "]";
    }

    return altered;
}

void Node::BroadcastProposalHonest(const Proposal& proposal, int totalNodes)
{
    Broadcast(MessageType::ProposalBroadcast, SerializeProposal(proposal), mCurrentEpoch, totalNodes);
}

void Node::BroadcastProposalEquivocating(const Proposal& proposal, int totalNodes)
{
    for (int i = 0; i < totalNodes; ++i)
    {
        if (i == mId)
        {
            continue;
        }

        Proposal perReceiverProposal = BuildEquivocatingProposalForReceiver(proposal, i);
        SendMessage(i, MessageType::ProposalBroadcast, SerializeProposal(perReceiverProposal), mCurrentEpoch);
    }
}

void Node::BroadcastEchoForProposal(const Proposal& proposal, int totalNodes)
{
    mEchoBroadcastedForProposer.insert(proposal.proposerId);
    RegisterEcho(proposal.proposerId, proposal, mId);
    TryDeliverRbcProposal(proposal.proposerId, proposal);
    Broadcast(MessageType::Echo, SerializeEchoPayload(proposal.proposerId, proposal), mCurrentEpoch, totalNodes);
    TryFinalizeCommonSubset();
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

std::string Node::SerializeEchoPayload(int proposerId, const Proposal& proposal) const
{
    return std::to_string(proposerId) + "\n" + SerializeProposal(proposal);
}

Proposal Node::DeserializeEchoPayload(const std::string& data, int& proposerId) const
{
    const std::size_t firstNewLine = data.find('\n');
    if (firstNewLine == std::string::npos)
    {
        throw std::runtime_error("Invalid echo payload format.");
    }

    proposerId = std::stoi(data.substr(0, firstNewLine));
    return DeserializeProposal(data.substr(firstNewLine + 1));
}
