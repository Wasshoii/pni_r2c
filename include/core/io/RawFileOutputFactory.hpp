#pragma once

#include <memory>

namespace openpni::distributed::acquisition {
    struct StorageConfig;
    class IRawFileOutput;
}

namespace openpni::distributed::coreio {
    std::unique_ptr<openpni::distributed::acquisition::IRawFileOutput> CreateRawFileOutput(const openpni::distributed::acquisition::StorageConfig &cfg);
}
