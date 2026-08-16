#pragma once

#include "dataplane/rdma/RdmaTypes.hpp"

#include <cstddef>
#include <memory>
#include <string>
#include <vector>

struct ibv_context;
struct ibv_pd;
struct ibv_cq;
struct ibv_mr;
struct ibv_qp;
struct ibv_device;

namespace openpni::distributed::dataplane::rdma
{

class RdmaDevice
{
public:
    static bool hasVerbsDevice();
    static std::vector<std::string> listDeviceNames();

    bool open(const std::string &deviceName = "", int gidIndex = -1);
    void close();

    ibv_context *context() const noexcept { return m_ctx; }
    ibv_pd *pd() const noexcept { return m_pd; }
    const std::string &name() const noexcept { return m_name; }
    uint8_t portNum() const noexcept { return m_port; }
    int gidIndex() const noexcept { return m_gidIndex; }
    bool ok() const noexcept { return m_ctx != nullptr && m_pd != nullptr; }

    bool queryGid(std::array<uint8_t, 16> *outGid, uint16_t *outLid) const;
    /** Active path MTU enum value (ibv_mtu); 0 on failure. */
    int queryActiveMtu() const;

private:
    ibv_context *m_ctx = nullptr;
    ibv_pd *m_pd = nullptr;
    std::string m_name;
    uint8_t m_port = 1;
    int m_gidIndex = 0;
};

struct RdmaQpEndpoints
{
    uint32_t qpNum = 0;
    uint32_t psn = 0;
};

struct RdmaWorkCompletion
{
    uint64_t wrId = 0;
    uint32_t immData = 0;
    int status = 0;
    bool isRecv = false;
    bool hasImm = false;
};

/**
 * Owns CQ + RC QP for a single connection. MR registration is separate.
 */
class RdmaConnection
{
public:
    ~RdmaConnection();

    bool create(RdmaDevice &dev, int cqEntries = 1024, int maxSendWr = 256, int maxRecvWr = 128);
    void destroy();

    ibv_mr *registerMemory(void *addr, size_t length, int accessFlags);
    void deregister(ibv_mr *mr);

    bool connectTo(const RdmaEndpointInfo &remote, uint8_t localPort, const std::array<uint8_t, 16> &localGid);
    bool transitionToInit(uint8_t port);
    bool transitionToRtr(const RdmaEndpointInfo &remote, uint8_t localPort, const std::array<uint8_t, 16> &localGid);
    bool transitionToRts(uint32_t localPsn);

    bool postWrite(const void *localAddr, uint32_t length, uint32_t lkey,
                   uint64_t remoteAddr, uint32_t rkey, uint64_t wrId, bool signaled);
    bool postWriteImm(const void *localAddr, uint32_t length, uint32_t lkey,
                      uint64_t remoteAddr, uint32_t rkey, uint32_t imm,
                      uint64_t wrId, bool signaled);
    bool postRecv(uint64_t wrId);
    bool postRecvBatch(int count);

    /** Poll up to maxCompletions. Returns number filled, or -1 on CQ error. */
    int pollCq(RdmaWorkCompletion *out, int maxCompletions);
    bool pollOne(bool *timedOut = nullptr, int timeoutMs = 0);
    /** Drain send completions until sendOutstanding==0 or a WC error. */
    bool drainSendCompletions();

    ibv_qp *qp() const noexcept { return m_qp; }
    ibv_cq *cq() const noexcept { return m_cq; }
    uint32_t qpNum() const;
    uint32_t localPsn() const noexcept { return m_localPsn; }
    int sendOutstanding() const noexcept { return m_sendOutstanding; }
    int maxSendWr() const noexcept { return m_maxSendWr; }
    int maxRecvWr() const noexcept { return m_maxRecvWr; }
    int postedRecvs() const noexcept { return m_postedRecvs; }

private:
    bool postWriteCommon(const void *localAddr, uint32_t length, uint32_t lkey,
                         uint64_t remoteAddr, uint32_t rkey, uint64_t wrId, bool signaled,
                         bool withImm, uint32_t imm);

    RdmaDevice *m_dev = nullptr;
    ibv_cq *m_cq = nullptr;
    ibv_qp *m_qp = nullptr;
    uint32_t m_localPsn = 0;
    int m_sendOutstanding = 0;
    int m_maxSendWr = 0;
    int m_maxRecvWr = 0;
    int m_postedRecvs = 0;
    int m_inlineBytes = 0;
};

} // namespace openpni::distributed::dataplane::rdma
