#include "core/io/RawFileOutputFactory.hpp"
#include "core/acquisition/AcquisitionServer.hpp"
#include "core/io/ShardedRawFileOutput.hpp"

namespace openpni::distributed::coreio
{
    std::unique_ptr<openpni::distributed::acquisition::IRawFileOutput> CreateRawFileOutput(const openpni::distributed::acquisition::StorageConfig &cfg)
    {
        if (!cfg.output_roots.empty())
        {
            return std::make_unique<ShardedRawFileOutput>(cfg);
        }
        return std::make_unique<openpni::distributed::acquisition::RollingRawFileOutput>(cfg);
    }

} // namespace openpni::distributed::coreio
