#include "dataplane/rdma/RdmaContext.hpp"

#include <algorithm>
#include <array>
#include <cstdlib>
#include <cstring>
#include <random>

#include <infiniband/verbs.h>
#include <iostream>
#define LOG(severity) ::std::cerr
#define VLOG(level) if(true) ; else ::std::cerr
#define LOG_EVERY_N(severity, n) ::std::cerr

namespace openpni::distributed::dataplane::rdma
{

bool RdmaDevice::hasVerbsDevice()
{
    int num = 0;
    ibv_device **list = ibv_get_device_list(&num);
    if (!list)
    {
        return false;
    }
    const bool ok = num > 0;
    ibv_free_device_list(list);
    return ok;
}

std::vector<std::string> RdmaDevice::listDeviceNames()
{
    std::vector<std::string> names;
    int num = 0;
    ibv_device **list = ibv_get_device_list(&num);
    if (!list)
    {
        return names;
    }
    for (int i = 0; i < num; ++i)
    {
        names.emplace_back(list[i]->name ? list[i]->name : "");
    }
    ibv_free_device_list(list);
    return names;
}

bool RdmaDevice::open(const std::string &deviceName, int gidIndex)
{
    close();
    int num = 0;
    ibv_device **list = ibv_get_device_list(&num);
    if (!list || num <= 0)
    {
        if (list)
        {
            ibv_free_device_list(list);
        }
        return false;
    }

    ibv_device *dev = nullptr;
    for (int i = 0; i < num; ++i)
    {
        const char *n = list[i]->name ? list[i]->name : "";
        if (deviceName.empty() || deviceName == n)
        {
            dev = list[i];
            m_name = n;
            break;
        }
    }
    if (!dev)
    {
        ibv_free_device_list(list);
        LOG(ERROR) << "RdmaDevice: device not found: " << deviceName;
        return false;
    }

    m_ctx = ibv_open_device(dev);
    ibv_free_device_list(list);
    if (!m_ctx)
    {
        LOG(ERROR) << "ibv_open_device failed";
        return false;
    }

    m_pd = ibv_alloc_pd(m_ctx);
    if (!m_pd)
    {
        LOG(ERROR) << "ibv_alloc_pd failed";
        close();
        return false;
    }

    m_port = 1;
    m_gidIndex = 0;
    if (gidIndex >= 0)
    {
        m_gidIndex = gidIndex;
    }
    else if (const char *env = std::getenv("OPENPNI_RDMA_GID_INDEX"))
    {
        m_gidIndex = std::atoi(env);
    }
    return true;
}

void RdmaDevice::close()
{
    if (m_pd)
    {
        ibv_dealloc_pd(m_pd);
        m_pd = nullptr;
    }
    if (m_ctx)
    {
        ibv_close_device(m_ctx);
        m_ctx = nullptr;
    }
    m_name.clear();
}

bool RdmaDevice::queryGid(std::array<uint8_t, 16> *outGid, uint16_t *outLid) const
{
    if (!m_ctx || !outGid)
    {
        return false;
    }
    ibv_port_attr attr{};
    if (ibv_query_port(m_ctx, m_port, &attr) != 0)
    {
        LOG(ERROR) << "ibv_query_port failed";
        return false;
    }
    if (outLid)
    {
        *outLid = attr.lid;
    }
    ibv_gid gid{};
    if (ibv_query_gid(m_ctx, m_port, m_gidIndex, &gid) != 0)
    {
        LOG(ERROR) << "ibv_query_gid failed index=" << m_gidIndex;
        return false;
    }
    std::memcpy(outGid->data(), &gid, 16);
    return true;
}

int RdmaDevice::queryActiveMtu() const
{
    if (!m_ctx)
    {
        return 0;
    }
    ibv_port_attr attr{};
    if (ibv_query_port(m_ctx, m_port, &attr) != 0)
    {
        return 0;
    }
    return static_cast<int>(attr.active_mtu);
}

RdmaConnection::~RdmaConnection()
{
    destroy();
}

bool RdmaConnection::create(RdmaDevice &dev, int cqEntries, int maxSendWr, int maxRecvWr)
{
    destroy();
    if (!dev.ok())
    {
        return false;
    }
    m_dev = &dev;
    m_maxSendWr = maxSendWr;
    m_maxRecvWr = maxRecvWr;
    m_cq = ibv_create_cq(dev.context(), cqEntries, nullptr, nullptr, 0);
    if (!m_cq)
    {
        LOG(ERROR) << "ibv_create_cq failed";
        return false;
    }

    auto tryCreateQp = [&](int inlineBytes) -> bool
    {
        ibv_qp_init_attr init{};
        init.send_cq = m_cq;
        init.recv_cq = m_cq;
        init.cap.max_send_wr = maxSendWr;
        init.cap.max_recv_wr = maxRecvWr;
        init.cap.max_send_sge = 2;
        init.cap.max_recv_sge = 1;
        init.cap.max_inline_data = inlineBytes;
        init.qp_type = IBV_QPT_RC;
        m_qp = ibv_create_qp(dev.pd(), &init);
        if (!m_qp)
        {
            return false;
        }
        m_inlineBytes = static_cast<int>(init.cap.max_inline_data);
        return true;
    };

    if (!tryCreateQp(64) && !tryCreateQp(0))
    {
        LOG(ERROR) << "ibv_create_qp failed";
        destroy();
        return false;
    }

    std::random_device rd;
    m_localPsn = rd() & 0xffffff;
    return true;
}

void RdmaConnection::destroy()
{
    if (m_qp)
    {
        ibv_destroy_qp(m_qp);
        m_qp = nullptr;
    }
    if (m_cq)
    {
        ibv_destroy_cq(m_cq);
        m_cq = nullptr;
    }
    m_dev = nullptr;
    m_sendOutstanding = 0;
    m_sendSqOccupancy = 0;
    m_unsignaledSinceSignal = 0;
    m_signaledBatchSizes.clear();
    m_postedRecvs = 0;
    m_maxSendWr = 0;
    m_maxRecvWr = 0;
    m_inlineBytes = 0;
}

ibv_mr *RdmaConnection::registerMemory(void *addr, size_t length, int accessFlags)
{
    if (!m_dev || !m_dev->pd())
    {
        return nullptr;
    }
    ibv_mr *mr = ibv_reg_mr(m_dev->pd(), addr, length, accessFlags);
    if (!mr)
    {
        LOG(ERROR) << "ibv_reg_mr failed len=" << length;
    }
    return mr;
}

void RdmaConnection::deregister(ibv_mr *mr)
{
    if (mr)
    {
        ibv_dereg_mr(mr);
    }
}

uint32_t RdmaConnection::qpNum() const
{
    return m_qp ? m_qp->qp_num : 0;
}

bool RdmaConnection::transitionToInit(uint8_t port)
{
    ibv_qp_attr attr{};
    attr.qp_state = IBV_QPS_INIT;
    attr.pkey_index = 0;
    attr.port_num = port;
    attr.qp_access_flags = IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_WRITE | IBV_ACCESS_REMOTE_READ;
    const int flags = IBV_QP_STATE | IBV_QP_PKEY_INDEX | IBV_QP_PORT | IBV_QP_ACCESS_FLAGS;
    if (ibv_modify_qp(m_qp, &attr, flags) != 0)
    {
        LOG(ERROR) << "QP INIT failed";
        return false;
    }
    return true;
}

bool RdmaConnection::transitionToRtr(const RdmaEndpointInfo &remote, uint8_t localPort,
                                     const std::array<uint8_t, 16> &localGid)
{
    (void)localGid;
    ibv_mtu mtu = IBV_MTU_1024;
    const int active = m_dev ? m_dev->queryActiveMtu() : 0;
    if (active >= static_cast<int>(IBV_MTU_4096))
    {
        mtu = IBV_MTU_4096;
    }
    else if (active >= static_cast<int>(IBV_MTU_2048))
    {
        mtu = IBV_MTU_2048;
    }
    else if (active >= static_cast<int>(IBV_MTU_1024))
    {
        mtu = IBV_MTU_1024;
    }
    else if (active >= static_cast<int>(IBV_MTU_512))
    {
        mtu = IBV_MTU_512;
    }

    ibv_qp_attr attr{};
    attr.qp_state = IBV_QPS_RTR;
    attr.path_mtu = mtu;
    attr.dest_qp_num = remote.qpNum;
    attr.rq_psn = remote.psn;
    attr.max_dest_rd_atomic = 1;
    attr.min_rnr_timer = 12;
    attr.ah_attr.is_global = 1;
    attr.ah_attr.dlid = static_cast<uint16_t>(remote.lid);
    attr.ah_attr.sl = 0;
    attr.ah_attr.src_path_bits = 0;
    attr.ah_attr.port_num = localPort;
    attr.ah_attr.grh.dgid = {};
    std::memcpy(&attr.ah_attr.grh.dgid, remote.gid.data(), 16);
    attr.ah_attr.grh.flow_label = 0;
    attr.ah_attr.grh.sgid_index = static_cast<uint8_t>(m_dev->gidIndex());
    attr.ah_attr.grh.hop_limit = 64;
    attr.ah_attr.grh.traffic_class = 0;

    const int flags = IBV_QP_STATE | IBV_QP_AV | IBV_QP_PATH_MTU | IBV_QP_DEST_QPN |
                      IBV_QP_RQ_PSN | IBV_QP_MAX_DEST_RD_ATOMIC | IBV_QP_MIN_RNR_TIMER;
    if (ibv_modify_qp(m_qp, &attr, flags) != 0)
    {
        LOG(ERROR) << "QP RTR failed";
        return false;
    }
    return true;
}

bool RdmaConnection::transitionToRts(uint32_t localPsn)
{
    ibv_qp_attr attr{};
    attr.qp_state = IBV_QPS_RTS;
    attr.timeout = 14;
    attr.retry_cnt = 7;
    attr.rnr_retry = 7;
    attr.sq_psn = localPsn;
    attr.max_rd_atomic = 1;
    const int flags = IBV_QP_STATE | IBV_QP_TIMEOUT | IBV_QP_RETRY_CNT | IBV_QP_RNR_RETRY |
                      IBV_QP_SQ_PSN | IBV_QP_MAX_QP_RD_ATOMIC;
    if (ibv_modify_qp(m_qp, &attr, flags) != 0)
    {
        LOG(ERROR) << "QP RTS failed";
        return false;
    }
    return true;
}

bool RdmaConnection::connectTo(const RdmaEndpointInfo &remote, uint8_t localPort,
                               const std::array<uint8_t, 16> &localGid)
{
    return transitionToInit(localPort) &&
           transitionToRtr(remote, localPort, localGid) &&
           transitionToRts(m_localPsn);
}

bool RdmaConnection::postWrite(const void *localAddr, uint32_t length, uint32_t lkey,
                               uint64_t remoteAddr, uint32_t rkey, uint64_t wrId, bool signaled)
{
    return postWriteCommon(localAddr, length, lkey, remoteAddr, rkey, wrId, signaled,
                           /*withImm=*/false, 0);
}

bool RdmaConnection::postWriteImm(const void *localAddr, uint32_t length, uint32_t lkey,
                                  uint64_t remoteAddr, uint32_t rkey, uint32_t imm,
                                  uint64_t wrId, bool signaled)
{
    return postWriteCommon(localAddr, length, lkey, remoteAddr, rkey, wrId, signaled,
                           /*withImm=*/true, imm);
}

bool RdmaConnection::postWriteCommon(const void *localAddr, uint32_t length, uint32_t lkey,
                                     uint64_t remoteAddr, uint32_t rkey, uint64_t wrId, bool signaled,
                                     bool withImm, uint32_t imm)
{
    if (!m_qp)
    {
        return false;
    }
    // Unsignaled WRs occupy the SQ but do not generate CQEs. Force a signal before
    // the queue fills, otherwise drainSendCompletions cannot make progress.
    if (!signaled && m_maxSendWr > 0 && m_sendSqOccupancy >= m_maxSendWr - 8)
    {
        signaled = true;
    }
    if (m_sendSqOccupancy >= m_maxSendWr - 1)
    {
        if (m_sendOutstanding <= 0)
        {
            signaled = true;
        }
        else if (!drainSendCompletions())
        {
            return false;
        }
    }

    ibv_sge sge{};
    sge.addr = reinterpret_cast<uintptr_t>(localAddr);
    sge.length = length;
    sge.lkey = lkey;

    ibv_send_wr wr{};
    wr.wr_id = wrId;
    wr.opcode = withImm ? IBV_WR_RDMA_WRITE_WITH_IMM : IBV_WR_RDMA_WRITE;
    wr.sg_list = &sge;
    wr.num_sge = 1;
    wr.send_flags = signaled ? IBV_SEND_SIGNALED : 0;
    wr.wr.rdma.remote_addr = remoteAddr;
    wr.wr.rdma.rkey = rkey;
    if (withImm)
    {
        wr.imm_data = imm;
    }

    ibv_send_wr *bad = nullptr;
    if (ibv_post_send(m_qp, &wr, &bad) != 0)
    {
        LOG(ERROR) << "ibv_post_send RDMA_WRITE failed";
        return false;
    }
    ++m_sendSqOccupancy;
    if (signaled)
    {
        m_signaledBatchSizes.push_back(m_unsignaledSinceSignal + 1);
        m_unsignaledSinceSignal = 0;
        ++m_sendOutstanding;
    }
    else
    {
        ++m_unsignaledSinceSignal;
    }
    return true;
}

bool RdmaConnection::postRecv(uint64_t wrId)
{
    if (!m_qp)
    {
        return false;
    }
    ibv_sge sge{};
    sge.addr = 0;
    sge.length = 0;
    sge.lkey = 0;
    ibv_recv_wr wr{};
    wr.wr_id = wrId;
    wr.num_sge = 1;
    wr.sg_list = &sge;
    ibv_recv_wr *bad = nullptr;
    if (ibv_post_recv(m_qp, &wr, &bad) != 0)
    {
        LOG(ERROR) << "ibv_post_recv failed";
        return false;
    }
    ++m_postedRecvs;
    return true;
}

bool RdmaConnection::postRecvBatch(int count)
{
    for (int i = 0; i < count; ++i)
    {
        if (!postRecv(static_cast<uint64_t>(i + 1)))
        {
            return false;
        }
    }
    return true;
}

int RdmaConnection::pollCq(RdmaWorkCompletion *out, int maxCompletions)
{
    if (!m_cq || !out || maxCompletions <= 0)
    {
        return 0;
    }
    ibv_wc wcs[32];
    const int nreq = std::min(maxCompletions, 32);
    const int n = ibv_poll_cq(m_cq, nreq, wcs);
    if (n < 0)
    {
        LOG(ERROR) << "ibv_poll_cq failed";
        return -1;
    }
    for (int i = 0; i < n; ++i)
    {
        out[i].wrId = wcs[i].wr_id;
        out[i].immData = wcs[i].imm_data;
        out[i].status = static_cast<int>(wcs[i].status);
        out[i].isRecv = (wcs[i].opcode == IBV_WC_RECV ||
                         wcs[i].opcode == IBV_WC_RECV_RDMA_WITH_IMM);
        out[i].hasImm = (wcs[i].wc_flags & IBV_WC_WITH_IMM) != 0;
        if (!out[i].isRecv)
        {
            if (m_sendOutstanding > 0)
            {
                --m_sendOutstanding;
            }
            if (!m_signaledBatchSizes.empty())
            {
                m_sendSqOccupancy -= m_signaledBatchSizes.front();
                m_signaledBatchSizes.pop_front();
                if (m_sendSqOccupancy < 0)
                {
                    m_sendSqOccupancy = 0;
                }
            }
            else if (m_sendSqOccupancy > 0)
            {
                --m_sendSqOccupancy;
            }
        }
        if (out[i].isRecv && m_postedRecvs > 0)
        {
            --m_postedRecvs;
        }
        if (wcs[i].status != IBV_WC_SUCCESS)
        {
            LOG(ERROR) << "CQ wc status=" << wcs[i].status
                       << " opcode=" << wcs[i].opcode
                       << " wr_id=" << wcs[i].wr_id;
        }
    }
    return n;
}

bool RdmaConnection::pollOne(bool *timedOut, int timeoutMs)
{
    (void)timeoutMs;
    if (timedOut)
    {
        *timedOut = false;
    }
    if (m_sendOutstanding <= 0)
    {
        return true;
    }
    RdmaWorkCompletion wc{};
    const int n = pollCq(&wc, 1);
    if (n < 0)
    {
        return false;
    }
    if (n == 0)
    {
        if (timedOut)
        {
            *timedOut = true;
        }
        return true;
    }
    return wc.status == 0;
}

bool RdmaConnection::drainSendCompletions()
{
    RdmaWorkCompletion wcs[16];
    int spins = 0;
    while (m_sendOutstanding > 0)
    {
        const int n = pollCq(wcs, 16);
        if (n < 0)
        {
            return false;
        }
        if (n == 0)
        {
            if (++spins > 10000000)
            {
                LOG(ERROR) << "drainSendCompletions timeout outstanding=" << m_sendOutstanding;
                return false;
            }
            continue;
        }
        for (int i = 0; i < n; ++i)
        {
            if (wcs[i].status != 0)
            {
                return false;
            }
        }
        spins = 0;
    }
    return true;
}

} // namespace openpni::distributed::dataplane::rdma
