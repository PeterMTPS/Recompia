// Two co-op channels on a fake session network. The link goes silent for 7 s (longer than kPeerTimeoutMs = 6000) and comes back
// while both processes stay in the session. Pass: both see each other as members again and a message goes through afterwards.
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstdio>
#include <cstring>
#include <deque>
#include <map>
#include <mutex>
#include <thread>
#include <cstdlib>
#include <vector>
#include "coop_channel.h"
using namespace nxrt::vnet;
using namespace std::chrono;

struct Net {
    std::mutex mu; std::condition_variable cv;
    std::map<uint32_t, std::deque<std::pair<uint32_t, std::vector<uint8_t>>>> q;
    std::atomic<bool> cut{false}; std::atomic<int> mode{0};  // 0 both ways, 1 only host->member, 2 only between nodes 0 and 1 (3 players)
};
static Net net;
static constexpr uint32_t kSub = (169u << 24) | (254u << 16) | (7u << 8);

struct FakePort : SessionPort {
    uint32_t ip;
    explicit FakePort(uint32_t i) : ip(i) {}
    uint32_t local_ip() const override { return ip; }
    uint32_t host_ip() const override { return kSub | 1; }
    bool send(uint32_t dst, const void* p, size_t n) override {
        std::lock_guard<std::mutex> lk(net.mu);
        std::vector<uint8_t> d(static_cast<const uint8_t*>(p), static_cast<const uint8_t*>(p) + n);
        for (auto& [to, qq] : net.q) {
            if (to == ip || !(dst == to || (dst & 0xFF) == 0xFF)) continue;
            if (net.cut) {   // lost on the way, as UDP is
                const uint32_t f = ip & 0xFF, t = to & 0xFF;
                if (net.mode == 0) continue;
                if (net.mode == 1 && f == 1 && t == 2) continue;
                if (net.mode == 2 && ((f == 1 && t == 2) || (f == 2 && t == 1))) continue;
            }
            qq.emplace_back(ip, d);
        }
        net.cv.notify_all();
        return true;
    }
    int recv(void* p, size_t cap, uint32_t* src, int timeout_ms) override {
        std::unique_lock<std::mutex> lk(net.mu);
        auto& qq = net.q[ip];
        net.cv.wait_for(lk, milliseconds(timeout_ms), [&] { return !qq.empty(); });
        if (qq.empty()) return 0;
        auto [s, d] = std::move(qq.front()); qq.pop_front();
        size_t n = std::min(cap, d.size()); memcpy(p, d.data(), n); *src = s; return static_cast<int>(n);
    }
};

static bool has(const CoopChannel& c, int node) { for (int m : c.members()) if (m == node) return true; return false; }
static bool wait_for(auto pred, int ms) {
    auto end = steady_clock::now() + milliseconds(ms);
    while (steady_clock::now() < end) { if (pred()) return true; std::this_thread::sleep_for(milliseconds(50)); }
    return pred();
}

int main(int argc, char** argv) {
    net.mode = argc > 1 ? atoi(argv[1]) : 0;
    const char* names[] = {"both ways", "one way (host -> member only)", "3 players, only nodes 0<->1 cut"};
    printf("mode: %s\n", names[net.mode]);
    { std::lock_guard<std::mutex> lk(net.mu); net.q[kSub | 1]; net.q[kSub | 2]; if (net.mode == 2) net.q[kSub | 3]; }
    CoopChannel a([](uint16_t) { return std::unique_ptr<SessionPort>(new FakePort(kSub | 1)); });   // host, node 0
    CoopChannel b([](uint16_t) { return std::unique_ptr<SessionPort>(new FakePort(kSub | 2)); });   // member, node 1
    std::unique_ptr<CoopChannel> c;
    if (net.mode == 2) c = std::make_unique<CoopChannel>([](uint16_t) { return std::unique_ptr<SessionPort>(new FakePort(kSub | 3)); });
    auto all = [&] { return has(a, 1) && has(b, 0) && (!c || (has(a, 2) && has(b, 2) && has(c.get()[0], 0) && has(c.get()[0], 1))); };
    bool ok = wait_for(all, 5000);
    printf("1. joined: %s\n", ok ? "yes" : "NO");
    if (!ok) return 2;
    puts("2. link silent for 7 s (both processes stay in the session)");
    net.cut = true; std::this_thread::sleep_for(milliseconds(7000)); net.cut = false;
    printf("   after the gap: host sees member %d, member sees host %d\n", has(a, 1), has(b, 0));
    bool back = wait_for(all, 10000);
    // stable: no endless leave/join churn once together (sample membership for 5 s)
    int flaps = 0; for (int i = 0; i < 50 && back; i++) { if (!all()) flaps++; std::this_thread::sleep_for(milliseconds(100)); }
    printf("   churn after recovery: %d of 50 samples not all together\n", flaps);
    back = back && flaps == 0;
    printf("3. back together within 10 s of the link returning: %s\n", back ? "yes" : "NO");
    bool msg = false;
    if (back) {
        b.send(0, 42, std::make_shared<const std::vector<uint8_t>>(std::vector<uint8_t>{1, 2, 3}));
        msg = wait_for([&] { for (auto& it : a.take(64)) if (it.kind == CoopChannel::Item::kMessage && it.type == 42) return true; return false; }, 3000);
    }
    printf("4. a message from the member reaches the host afterwards: %s\n", msg ? "yes" : "NO");
    a.stop(); b.stop(); if (c) c->stop();
    puts(back && msg ? "PASS" : "FAIL");
    return back && msg ? 0 : 1;
}
