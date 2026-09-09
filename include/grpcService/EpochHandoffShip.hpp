#pragma once

#include "core/streaming/StreamingCoincidence.hpp"
#include "dataplane/rdma/RdmaRecvServer.hpp"
#include "dataplane/rdma/RdmaWriteSender.hpp"

#include <cstdint>

namespace openpni::distributed::streaming
{

    /** Coin-A → Coin-B overlap+tail dataplane (InProcess memcpy or local RoCE). */
    bool sendEpochHandoffViaRdma(
        openpni::distributed::dataplane::rdma::RdmaWriteSender &tx,
        const EpochHandoff &handoff);

    /**
     * Handshake a dedicated ship QP (not the worker ingest path) and copy
     * `handoff` from A to B. `requireRoce` uses verbs when a device exists.
     */
    bool shipEpochHandoffViaDataplane(
        const EpochHandoff &handoff,
        EpochHandoff *out,
        bool requireRoce);

} // namespace openpni::distributed::streaming
