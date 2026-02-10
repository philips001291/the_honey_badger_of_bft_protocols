#include <algorithm>
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <functional>
#include <iomanip>
#include <iostream>
#include <mutex>
#include <optional>
#include <queue>
#include <random>
#include <set>
#include <string>
#include <thread>
#include <unordered_map>
#include <unordered_set>
#include <vector>

// ------------------------------------------------------------
// Async network simulator (random delays, unordered delivery)
// ------------------------------------------------------------

struct Message {
    std::string typ;
    int src = -1;
    int dst = -1;

    // Routing fields
    int epoch = -1;   // r
    int index = -1;   // sender idx or BA idx
    int round = -1;   // BA round

    // Generic payload
    std::string text;                 // small payload (e.g., "VAL")
    std::vector<std::string> list;    // tx proposal, etc.
};

class Mailbox {
public:
    void push(Message m) {
        std::lock_guard<std::mutex> lk(mu_);
        q_.push(std::move(m));
        cv_.notify_one();
    }

    // blocking pop
    Message pop() {
        std::unique_lock<std::mutex> lk(mu_);
        cv_.wait(lk, [&] { return !q_.empty() || stop_; });
        if (stop_) return Message{ "STOP", -1, -1 };
        auto m = std::move(q_.front());
        q_.pop();
        return m;
    }

    void stop() {
        std::lock_guard<std::mutex> lk(mu_);
        stop_ = true;
        cv_.notify_all();
    }

private:
    std::mutex mu_;
    std::condition_variable cv_;
    std::queue<Message> q_;
    bool stop_ = false;
};

class Network {
public:
    Network(int n, int maxDelayMs)
        : n_(n), maxDelayMs_(maxDelayMs), inbox_(n) {}

    void send(const Message& msg) {
        // deliver in background with random delay
        std::thread([this, msg]() mutable {
            int d = randInt(0, maxDelayMs_);
            std::this_thread::sleep_for(std::chrono::milliseconds(d));
            inbox_[msg.dst].push(std::move(msg));
            }).detach();
    }

    void broadcast(int src, const std::string& typ, int epoch, int index,
        int round, const std::string& text,
        const std::vector<std::string>& list = {}) {
        for (int dst = 0; dst < n_; dst++) {
            Message m;
            m.typ = typ;
            m.src = src;
            m.dst = dst;
            m.epoch = epoch;
            m.index = index;
            m.round = round;
            m.text = text;
            m.list = list;
            send(m);
        }
    }

    Mailbox& inbox(int i) { return inbox_[i]; }
    int n() const { return n_; }

private:
    int n_;
    int maxDelayMs_;
    std::vector<Mailbox> inbox_;

    static int randInt(int lo, int hi) {
        thread_local std::mt19937 rng{ std::random_device{}() };
        std::uniform_int_distribution<int> dist(lo, hi);
        return dist(rng);
    }
};

// ------------------------------------------------------------
// RBC (Reliable Broadcast) simplified: VAL/ECHO/READY
// Mirrors Figure 2 structure conceptually (no erasure/Merkle).
// ------------------------------------------------------------

class RBC {
public:
    RBC(Network& net, int n, int f, int me, int epoch, int sender)
        : net_(net), n_(n), f_(f), me_(me), epoch_(epoch), sender_(sender) {}

    void input(const std::vector<std::string>& v) {
        // only sender calls input
        if (me_ != sender_) return;
        // VAL to all
        net_.broadcast(me_, "RBC_VAL", epoch_, sender_, -1, "", v);
    }

    // returns true if delivered now
    bool handle(const Message& m) {
        if (m.epoch != epoch_ || m.index != sender_) return false;

        if (m.typ == "RBC_VAL") {
            if (!val_.has_value()) val_ = m.list;
            // ECHO to all
            net_.broadcast(me_, "RBC_ECHO", epoch_, sender_, -1, "", *val_);
            return false;
        }

        if (m.typ == "RBC_ECHO") {
            if (!val_.has_value()) val_ = m.list;
            if (m.list != *val_) return false;
            echoFrom_.insert(m.src);

            if (!sentReady_ && (int)echoFrom_.size() >= (n_ - f_)) {
                sentReady_ = true;
                net_.broadcast(me_, "RBC_READY", epoch_, sender_, -1, "", *val_);
            }
            return false;
        }

        if (m.typ == "RBC_READY") {
            if (!val_.has_value()) val_ = m.list;
            if (m.list != *val_) return false;
            readyFrom_.insert(m.src);

            if (!sentReady_ && (int)readyFrom_.size() >= (f_ + 1)) {
                sentReady_ = true;
                net_.broadcast(me_, "RBC_READY", epoch_, sender_, -1, "", *val_);
            }

            if (!delivered_ && (int)readyFrom_.size() >= (2 * f_ + 1)) {
                delivered_ = true;
                deliveredValue_ = *val_;
                return true;
            }
            return false;
        }

        return false;
    }

    bool delivered() const { return delivered_; }
    const std::vector<std::string>& value() const { return deliveredValue_; }

private:
    Network& net_;
    int n_, f_, me_, epoch_, sender_;

    std::optional<std::vector<std::string>> val_;
    std::unordered_set<int> echoFrom_;
    std::unordered_set<int> readyFrom_;
    bool sentReady_ = false;

    bool delivered_ = false;
    std::vector<std::string> deliveredValue_;
};

// ------------------------------------------------------------
// BA (Binary Agreement) simplified (random coin rounds).
// Paper uses common-coin BA; we simulate the idea.
// ------------------------------------------------------------

class BA {
public:
    BA(Network& net, int n, int f, int me, int epoch, int idx)
        : net_(net), n_(n), f_(f), me_(me), epoch_(epoch), idx_(idx) {}

    void input(int b) {
        if (hasInput_) return;
        hasInput_ = true;
        est_ = b;
        round_ = 1;
        broadcastVote();
    }

    // returns true if decided now
    bool handle(const Message& m) {
        if (m.epoch != epoch_ || m.index != idx_ || m.typ != "BA_VOTE") return false;

        int rd = m.round;
        auto& mp = votes_[rd];
        mp[m.src] = (m.text == "1" ? 1 : 0);

        if (decided_) return false;

        // when have N-f votes, check 2f+1 majority
        if ((int)mp.size() >= (n_ - f_)) {
            int ones = 0;
            for (auto& [_, bit] : mp) ones += bit;
            int zeros = (int)mp.size() - ones;

            if (ones >= (2 * f_ + 1)) {
                decided_ = true;
                decision_ = 1;
                return true;
            }
            if (zeros >= (2 * f_ + 1)) {
                decided_ = true;
                decision_ = 0;
                return true;
            }

            // otherwise next round with "coin"
            round_++;
            est_ = randBit();
            broadcastVote();
        }

        return false;
    }

    bool decided() const { return decided_; }
    int decision() const { return decision_; }
    bool hasInput() const { return hasInput_; }

private:
    Network& net_;
    int n_, f_, me_, epoch_, idx_;
    bool hasInput_ = false;

    int est_ = 0;
    int round_ = 0;

    bool decided_ = false;
    int decision_ = -1;

    std::unordered_map<int, std::unordered_map<int, int>> votes_; // round -> {src:bit}

    void broadcastVote() {
        net_.broadcast(me_, "BA_VOTE", epoch_, idx_, round_, (est_ ? "1" : "0"));
    }

    static int randBit() {
        thread_local std::mt19937 rng{ std::random_device{}() };
        std::uniform_int_distribution<int> dist(0, 1);
        return dist(rng);
    }
};

// ------------------------------------------------------------
// ACS (Asynchronous Common Subset): RBC + BA as Figure 4.
// - Run N RBCs (each sender)
// - When RBC[j] delivers -> input 1 to BA[j]
// - After (N-f) BA decisions are 1 -> input 0 to remaining BA
// - Output set C of indices with BA=1, and union RBC values for j in C
// ------------------------------------------------------------

class ACS {
public:
    ACS(Network& net, int n, int f, int me, int epoch)
        : net_(net), n_(n), f_(f), me_(me), epoch_(epoch) {
        rbcs_.reserve(n_);
        bas_.reserve(n_);
        for (int j = 0; j < n_; j++) {
            rbcs_.emplace_back(net_, n_, f_, me_, epoch_, j);
            bas_.emplace_back(net_, n_, f_, me_, epoch_, j);
        }
    }

    void input_my_value(const std::vector<std::string>& v) {
        rbcs_[me_].input(v);
    }

    // Drive ACS until output ready. Call this from node's event loop.
    // Returns std::optional<map index->value> once completed.
    std::optional<std::unordered_map<int, std::vector<std::string>>> handle(const Message& m) {
        if (m.epoch != epoch_) return std::nullopt;

        // route RBC
        if (startsWith(m.typ, "RBC_")) {
            int sender = m.index;
            if (sender < 0 || sender >= n_) return std::nullopt;

            bool deliveredNow = rbcs_[sender].handle(m);
            if (deliveredNow) {
                // upon delivery of vj from RBCj -> input 1 to BAj (Figure 4) :contentReference[oaicite:2]{index=2}
                if (!bas_[sender].hasInput()) bas_[sender].input(1);
            }
        }

        // route BA
        if (m.typ == "BA_VOTE") {
            int idx = m.index;
            if (idx < 0 || idx >= n_) return std::nullopt;

            bool decidedNow = bas_[idx].handle(m);
            if (decidedNow) {
                // track decisions
                if (bas_[idx].decision() == 1) decidedOnes_.insert(idx);
            }
        }

        // once we have >= (N-f) ones, provide 0 to any BA without input (Figure 4) :contentReference[oaicite:3]{index=3}
        if (!fedZeros_ && (int)decidedOnes_.size() >= (n_ - f_)) {
            fedZeros_ = true;
            for (int j = 0; j < n_; j++) {
                if (!bas_[j].hasInput()) bas_[j].input(0);
            }
        }

        // when all BA decided, output C and wait RBCs in C delivered
        if (!outputReady_) {
            bool allDecided = true;
            for (int j = 0; j < n_; j++) {
                if (!bas_[j].decided()) { allDecided = false; break; }
            }
            if (allDecided) {
                C_.clear();
                for (int j = 0; j < n_; j++) {
                    if (bas_[j].decision() == 1) C_.insert(j);
                }
                // ensure RBC delivered for all j in C
                bool allRbc = true;
                for (int j : C_) {
                    if (!rbcs_[j].delivered()) { allRbc = false; break; }
                }
                if (allRbc) {
                    outputReady_ = true;
                    std::unordered_map<int, std::vector<std::string>> out;
                    for (int j : C_) out[j] = rbcs_[j].value();
                    return out;
                }
            }
        }

        return std::nullopt;
    }

private:
    Network& net_;
    int n_, f_, me_, epoch_;

    std::vector<RBC> rbcs_;
    std::vector<BA> bas_;

    bool fedZeros_ = false;
    bool outputReady_ = false;

    std::unordered_set<int> decidedOnes_;
    std::unordered_set<int> C_;

    static bool startsWith(const std::string& s, const std::string& p) {
        return s.size() >= p.size() && s.compare(0, p.size(), p) == 0;
    }
};

// ------------------------------------------------------------
// HoneyBadgerBFT Node (epoch):
// - choose random subset of B/N from first B of buf
// - "encrypt" stub: we just send a ciphertext marker in ACS,
//   then later we REVEAL plaintext proposal (commit-reveal)
// - after ACS picks set C, request reveals from proposers in C,
//   build block = sorted union, remove from buf
// ------------------------------------------------------------

class HBNode {
public:
    HBNode(Network& net, int n, int f, int me, int B)
        : net_(net), n_(n), f_(f), me_(me), B_(B) {}

    void addTxs(const std::vector<std::string>& txs) {
        for (auto& t : txs) buf_.push_back(t);
    }

    void startEpoch(int r) {
        epoch_ = r;
        acs_.emplace(net_, n_, f_, me_, r);

        // Step 1: pick proposal
        int k = std::max(1, B_ / n_);
        std::vector<std::string> window;
        for (int i = 0; i < (int)buf_.size() && i < B_; i++) window.push_back(buf_[i]);
        proposal_ = sample(window, k);

        // store reveal locally
        reveals_[r] = proposal_;

        // feed ACS my "ciphertext" (we send a marker, real TPKE would hide plaintext)
        acs_->input_my_value({ "CT_FROM_" + std::to_string(me_) });

        // We also proactively broadcast RBC_VAL for my ciphertext marker via ACS input,
        // which is already done inside acs_->input_my_value (sender's RBC input).
    }

    // event loop: handle messages; returns optional committed block when epoch completes
    std::optional<std::vector<std::string>> step() {
        Message m = net_.inbox(me_).pop();
        if (m.typ == "STOP") return std::nullopt;

        // Handle reveal requests
        if (m.typ == "REVEAL_REQ" && m.epoch == epoch_) {
            int r = m.epoch;
            auto it = reveals_.find(r);
            std::vector<std::string> prop = (it != reveals_.end() ? it->second : std::vector<std::string>{});
            Message ans;
            ans.typ = "REVEAL";
            ans.src = me_;
            ans.dst = m.src;
            ans.epoch = r;
            ans.list = prop;
            net_.send(ans);
            return std::nullopt;
        }

        // Handle reveals we are waiting for
        if (m.typ == "REVEAL" && m.epoch == epoch_) {
            if (waitingReveals_.count(m.src)) {
                waitingReveals_.erase(m.src);
                for (auto& tx : m.list) revealedTxs_.push_back(tx);
            }
            // If we received all reveals, commit
            if (waitingForReveals_ && waitingReveals_.empty()) {
                waitingForReveals_ = false;

                // build block
                std::sort(revealedTxs_.begin(), revealedTxs_.end());
                revealedTxs_.erase(std::unique(revealedTxs_.begin(), revealedTxs_.end()), revealedTxs_.end());

                // remove from buffer
                std::unordered_set<std::string> s(revealedTxs_.begin(), revealedTxs_.end());
                std::vector<std::string> nb;
                nb.reserve(buf_.size());
                for (auto& tx : buf_) if (!s.count(tx)) nb.push_back(tx);
                buf_.swap(nb);

                committedBlocks_.push_back(revealedTxs_);
                return revealedTxs_;
            }
            return std::nullopt;
        }

        // Route to ACS
        if (acs_.has_value()) {
            auto out = acs_->handle(m);
            if (out.has_value() && !waitingForReveals_) {
                // ACS finished: out is indices C -> ciphertext markers
                // Now request reveals from indices in C (simulate threshold decrypt step)
                waitingReveals_.clear();
                revealedTxs_.clear();
                for (auto& [j, _] : *out) {
                    waitingReveals_.insert(j);
                    Message req;
                    req.typ = "REVEAL_REQ";
                    req.src = me_;
                    req.dst = j;
                    req.epoch = epoch_;
                    net_.send(req);
                }
                waitingForReveals_ = true;
            }
        }

        return std::nullopt;
    }

    const std::vector<std::vector<std::string>>& blocks() const { return committedBlocks_; }

private:
    Network& net_;
    int n_, f_, me_, B_;
    int epoch_ = -1;

    std::vector<std::string> buf_;

    std::optional<ACS> acs_;

    std::vector<std::string> proposal_;
    std::unordered_map<int, std::vector<std::string>> reveals_; // epoch -> plaintext proposal

    bool waitingForReveals_ = false;
    std::unordered_set<int> waitingReveals_;
    std::vector<std::string> revealedTxs_;

    std::vector<std::vector<std::string>> committedBlocks_;

    static std::vector<std::string> sample(const std::vector<std::string>& v, int k) {
        std::vector<std::string> out;
        if (v.empty()) return out;
        k = std::min(k, (int)v.size());
        std::vector<int> idx(v.size());
        for (int i = 0; i < (int)v.size(); i++) idx[i] = i;
        std::shuffle(idx.begin(), idx.end(), rng());
        for (int i = 0; i < k; i++) out.push_back(v[idx[i]]);
        return out;
    }

    static std::mt19937& rng() {
        thread_local std::mt19937 gen{ std::random_device{}() };
        return gen;
    }
};

// ------------------------------------------------------------
// Demo main: N=4, f=1, run 2 epochs, print blocks per node
// ------------------------------------------------------------

int main() {
    int N = 4;
    int f = 1;
    int B = 20;
    int epochs = 2;

    Network net(N, /*maxDelayMs=*/30);

    std::vector<HBNode> nodes;
    nodes.reserve(N);
    for (int i = 0; i < N; i++) nodes.emplace_back(net, N, f, i, B);

    // Give all nodes same tx pool (overlap on purpose)
    std::vector<std::string> txs;
    for (int t = 1; t <= 30; t++) txs.push_back("tx" + std::to_string(t));
    for (int i = 0; i < N; i++) nodes[i].addTxs(txs);

    // Run epochs sequentially; within each epoch run all nodes until each commits
    for (int r = 0; r < epochs; r++) {
        for (int i = 0; i < N; i++) nodes[i].startEpoch(r);

        std::vector<std::optional<std::vector<std::string>>> committed(N);
        int done = 0;

        // naive driving loop: keep stepping nodes until all committed block for this epoch
        while (done < N) {
            for (int i = 0; i < N; i++) {
                if (committed[i].has_value()) continue;
                auto blk = nodes[i].step();
                if (blk.has_value()) {
                    committed[i] = blk.value();
                    done++;
                }
            }
        }

        std::cout << "\nEpoch " << r << " committed blocks:\n";
        for (int i = 0; i < N; i++) {
            std::cout << "  node " << i << ": [";
            auto& b = committed[i].value();
            for (size_t k = 0; k < b.size() && k < 10; k++) {
                std::cout << b[k] << (k + 1 < b.size() && k + 1 < 10 ? ", " : "");
            }
            if (b.size() > 10) std::cout << ", ...";
            std::cout << "] (size=" << b.size() << ")\n";
        }
    }

    // stop inboxes (not strictly needed in this demo)
    for (int i = 0; i < N; i++) net.inbox(i).stop();

    std::cout << "\nDone.\n";
    return 0;
}
