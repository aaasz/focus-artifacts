#ifndef YCSB_C_NUMAUTILS_H_
#define YCSB_C_NUMAUTILS_H_

#include <thread>
#include <vector>
#include <cassert>
#include <numa.h>
#include <sstream>
#include <cstring>

namespace numautils {

struct CPUTopology {
    uint32_t physical_cores;
    uint32_t threads_per_core;
    uint32_t total_threads;
};

inline CPUTopology get_cpu_topology() {
    CPUTopology topology = {0, 0, 0};
    
    #ifdef __linux__
    FILE* fp = popen("lscpu | grep -E '^CPU\\(s\\)|^Thread|^Core'", "r");
    if (fp != nullptr) {
        char line[256];
        while (fgets(line, sizeof(line), fp)) {
            if (strstr(line, "CPU(s):")) {
                sscanf(line, "CPU(s): %u", &topology.total_threads);
            } else if (strstr(line, "Thread(s) per core:")) {
                sscanf(line, "Thread(s) per core: %u", &topology.threads_per_core);
            } else if (strstr(line, "Core(s) per socket:")) {
                uint32_t cores_per_socket;
                sscanf(line, "Core(s) per socket: %u", &cores_per_socket);
                // Also need to get number of sockets
                FILE* socket_fp = popen("lscpu | grep '^Socket'", "r");
                if (socket_fp != nullptr) {
                    char socket_line[256];
                    if (fgets(socket_line, sizeof(socket_line), socket_fp)) {
                        uint32_t num_sockets;
                        sscanf(socket_line, "Socket(s): %u", &num_sockets);
                        topology.physical_cores = cores_per_socket * num_sockets;
                    }
                    pclose(socket_fp);
                }
            }
        }
        pclose(fp);
    }
    #else
    // Fallback to hardware_concurrency for non-Linux systems
    topology.total_threads = std::thread::hardware_concurrency();
    topology.threads_per_core = 1; // Conservative assumption
    topology.physical_cores = topology.total_threads;
    #endif

    return topology;
}

std::vector<size_t>
get_cores(const int max_cpus_wanted)
{
   if (max_cpus_wanted > numa_num_configured_cpus()) {
      throw std::runtime_error("Requested more CPUs than available");
   }
   std::vector<size_t> cores;
   int                 num_cpus_per_node =
      numa_num_configured_cpus() / numa_num_configured_nodes();
   bool use_single_node = (num_cpus_per_node >= max_cpus_wanted);
   for (int i = 0; (int)cores.size() < max_cpus_wanted
                   && (int)cores.size() < numa_num_configured_cpus();)
   {
      std::ifstream core_cpus_list_file("/sys/devices/system/cpu/cpu"
                                        + std::to_string(i)
                                        + "/topology/core_cpus_list");

      std::ifstream physical_package_id_file("/sys/devices/system/cpu/cpu"
                                             + std::to_string(i)
                                             + "/topology/physical_package_id");
      if (core_cpus_list_file && physical_package_id_file) {
         if (use_single_node) {
            int physical_package_id;
            physical_package_id_file >> physical_package_id;
            if (physical_package_id != 0) {
               i++;
               continue;
            }
         }
         // It assumes two cpus per core
         std::string line;
         std::getline(core_cpus_list_file, line);
         std::istringstream iss(line);
         std::string        token;
         std::getline(iss, token, ',');
         cores.push_back(std::stoi(token));
         std::getline(iss, token, ',');
         cores.push_back(std::stoi(token));
      } else {
         throw std::runtime_error("Failed to read core cpus list for CPU "
                                  + std::to_string(i));
      }
      i++;
   }
   return cores;
}

size_t
bind_to_core(std::thread &thr, size_t cpu_num)
{
   cpu_set_t cpuset;
   CPU_ZERO(&cpuset);
   CPU_SET(cpu_num, &cpuset);

   int rc =
      pthread_setaffinity_np(thr.native_handle(), sizeof(cpu_set_t), &cpuset);
   if (rc != 0) {
      std::cerr << "Error calling pthread_setaffinity_np: " << rc << "\n";
   }

   return cpu_num;
}

void
clear_affinity_for_process()
{
   cpu_set_t mask;
   CPU_ZERO(&mask);
   const size_t num_cpus = std::thread::hardware_concurrency();
   for (size_t i = 0; i < num_cpus; i++)
      CPU_SET(i, &mask);

   int ret = sched_setaffinity(0 /* whole-process */, sizeof(cpu_set_t), &mask);
   assert(ret == 0);
}

} // namespace numautils
#endif