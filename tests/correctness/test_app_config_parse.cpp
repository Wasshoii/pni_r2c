/**
 * Parse example + InProcess smoke JSON via AppConfig.
 * If app binaries exist, also run --dry-run (skip if not built).
 */

#include "app/common/AppConfig.hpp"

#include <cstdlib>
#include <iostream>
#include <string>
#include <sys/wait.h>
#include <unistd.h>
#include <vector>

namespace appcfg = openpni::distributed::app;

namespace
{
    bool fail(const char *msg)
    {
        std::cerr << "[FAIL] app_config: " << msg << "\n";
        return false;
    }

    bool fileReadable(const std::string &path)
    {
        return access(path.c_str(), R_OK) == 0;
    }

    bool fileExecutable(const std::string &path)
    {
        return access(path.c_str(), X_OK) == 0;
    }

    std::string findAppBin(const std::string &name)
    {
        std::vector<std::string> cands;
        if (const char *env = std::getenv("R2C_BIN_DIR"))
        {
            cands.push_back(std::string(env) + "/app/" + name);
        }
        cands.push_back("bin/app/" + name);
        cands.push_back("build/apps/basic/bin/app/" + name);
        cands.push_back("build/apps/cuda/bin/app/" + name);
        for (const auto &p : cands)
        {
            if (fileExecutable(p))
            {
                return p;
            }
        }
        return {};
    }

    bool testParseExamples()
    {
        appcfg::AcqR2SNodeConfig worker;
        std::string err;
        if (!appcfg::loadAcqR2SNodeConfig("app/config/examples/acq_r2s_node.example.json", &worker, &err))
        {
            std::cerr << err << "\n";
            return fail("load acq_r2s_node.example.json");
        }
        if (worker.source.type != appcfg::WorkerSourceType::Synthetic)
        {
            return fail("example worker source.type != synthetic");
        }
        if (worker.source.promptPairs != 10000 || worker.source.delayPairs != 10000)
        {
            return fail("example worker pair counts");
        }
        if (!worker.coinClient.dataplane.requireRoce || worker.coinClient.dataplane.forceInProcess)
        {
            return fail("example worker dataplane should require RoCE");
        }
        if (worker.coinClient.serverAddress != "127.0.0.1:50061" || worker.coinClient.nodeId != 0)
        {
            return fail("example worker cluster fields");
        }

        appcfg::CoinMasterConfig coin;
        if (!appcfg::loadCoinMasterConfig("app/config/examples/coin_master.example.json", &coin, &err))
        {
            std::cerr << err << "\n";
            return fail("load coin_master.example.json");
        }
        if (coin.coinMaster.listenAddress != "0.0.0.0:50061" || coin.coinMaster.expectedNodeCount != 1)
        {
            return fail("example coin cluster fields");
        }
        if (!coin.coinMaster.dataplane.requireRoce || coin.coinMaster.dataplane.forceInProcess)
        {
            return fail("example coin dataplane should require RoCE");
        }
        if (coin.coinMaster.detectorProfile != "BDM2")
        {
            return fail("example coin detectorProfile");
        }
        if (coin.aligner.coinProtocol.timeWindowPs != 2000)
        {
            return fail("example coin protocol.timeWindowPs");
        }
        std::cout << "[PASS] parse_example_json\n";
        return true;
    }

    bool testParseInProcessSmoke()
    {
        appcfg::CoinMasterConfig coin;
        std::string err;
        if (!appcfg::loadCoinMasterConfig("app/config/tests/inprocess_smoke/coin.json", &coin, &err))
        {
            std::cerr << err << "\n";
            return fail("load inprocess_smoke/coin.json");
        }
        if (coin.coinMaster.dataplane.requireRoce || !coin.coinMaster.dataplane.forceInProcess)
        {
            return fail("smoke coin must be InProcess");
        }
        if (coin.coinMaster.expectedNodeCount != 2)
        {
            return fail("smoke coin expectedNodeCount");
        }

        appcfg::AcqR2SNodeConfig w0;
        if (!appcfg::loadAcqR2SNodeConfig("app/config/tests/inprocess_smoke/worker0.json", &w0, &err))
        {
            std::cerr << err << "\n";
            return fail("load inprocess_smoke/worker0.json");
        }
        appcfg::AcqR2SNodeConfig w1;
        if (!appcfg::loadAcqR2SNodeConfig("app/config/tests/inprocess_smoke/worker1.json", &w1, &err))
        {
            std::cerr << err << "\n";
            return fail("load inprocess_smoke/worker1.json");
        }
        if (w0.coinClient.dataplane.requireRoce || !w0.coinClient.dataplane.forceInProcess ||
            w1.coinClient.dataplane.requireRoce || !w1.coinClient.dataplane.forceInProcess)
        {
            return fail("smoke workers must be InProcess");
        }
        if (w0.source.promptPairs != 64 || w0.source.delayPairs != 64 ||
            w1.source.promptPairs != 64 || w1.source.delayPairs != 64)
        {
            return fail("smoke worker pair counts");
        }
        if (w0.coinClient.nodeId != 0 || w1.coinClient.nodeId != 1)
        {
            return fail("smoke worker nodeId");
        }
        std::cout << "[PASS] parse_inprocess_smoke_json\n";
        return true;
    }

    bool runDryRun(const std::string &bin, const std::string &configPath)
    {
        const pid_t pid = fork();
        if (pid < 0)
        {
            return fail("fork dry-run");
        }
        if (pid == 0)
        {
            setenv("GLOG_logtostderr", "1", 1);
            const char *argv[] = {bin.c_str(), "--config", configPath.c_str(), "--dry-run", nullptr};
            execv(bin.c_str(), const_cast<char **>(argv));
            _exit(127);
        }
        int status = 0;
        if (waitpid(pid, &status, 0) < 0)
        {
            return fail("waitpid dry-run");
        }
        if (!WIFEXITED(status) || WEXITSTATUS(status) != 0)
        {
            std::cerr << "dry-run " << bin << " " << configPath << " status=" << status << "\n";
            return fail("dry-run exit code");
        }
        return true;
    }

    bool testAppDryRunIfPresent()
    {
        const std::string coinBin = findAppBin("app_coin_master");
        const std::string workerBin = findAppBin("app_acq_r2s_node");
        if (coinBin.empty() && workerBin.empty())
        {
            std::cout << "[SKIP] app --dry-run (bin/app not built)\n";
            return true;
        }
        if (!coinBin.empty())
        {
            if (!fileReadable("app/config/examples/coin_master.example.json") ||
                !fileReadable("app/config/tests/inprocess_smoke/coin.json"))
            {
                return fail("coin config missing for dry-run");
            }
            if (!runDryRun(coinBin, "app/config/examples/coin_master.example.json") ||
                !runDryRun(coinBin, "app/config/tests/inprocess_smoke/coin.json"))
            {
                return false;
            }
        }
        else
        {
            std::cout << "[SKIP] app_coin_master --dry-run (binary missing)\n";
        }
        if (!workerBin.empty())
        {
            if (!fileReadable("app/config/examples/acq_r2s_node.example.json") ||
                !fileReadable("app/config/tests/inprocess_smoke/worker0.json"))
            {
                return fail("worker config missing for dry-run");
            }
            if (!runDryRun(workerBin, "app/config/examples/acq_r2s_node.example.json") ||
                !runDryRun(workerBin, "app/config/tests/inprocess_smoke/worker0.json"))
            {
                return false;
            }
        }
        else
        {
            std::cout << "[SKIP] app_acq_r2s_node --dry-run (binary missing)\n";
        }
        std::cout << "[PASS] app_dry_run\n";
        return true;
    }
} // namespace

int main()
{
    int rc = 0;
    if (!testParseExamples())
    {
        rc = 1;
    }
    if (!testParseInProcessSmoke())
    {
        rc = 1;
    }
    if (!testAppDryRunIfPresent())
    {
        rc = 1;
    }
    return rc;
}
