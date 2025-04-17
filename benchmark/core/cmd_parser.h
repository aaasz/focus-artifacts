#pragma once

#include <cstring>
#include <fstream>
#include <iostream>
#include <map>
#include <string>
#include <vector>
#include "benchmark_structs.h"
#include "utils.h"

namespace ycsbc {

extern std::map<std::string, std::string> default_props;

static inline bool StrStartWith(const char *str, const char *pre) {
   return strncmp(str, pre, strlen(pre)) == 0;
}

static void UsageMessage(const char *command) {
   std::cout << "Usage: " << command << " [options]"
             << "-L <load-workload.spec> [-W run-workload.spec] ..." << std::endl;
   std::cout << "       Perform the given Load workload, then each Run workload"
             << std::endl;
   std::cout << "Usage: " << command << " [options]"
             << "-P <load-workload.spec> [-W run-workload.spec] ... " << std::endl;
   std::cout << "       Perform each given Run workload on a database that has been "
                "preloaded with the given Load workload"
             << std::endl;
   std::cout << "Options:" << std::endl;
   std::cout << "  -threads <n>: execute using <n> threads (default: "
             << default_props["threadcount"] << ")" << std::endl;
   std::cout << "  -db <dbname>: specify the name of the DB to use (default: "
             << default_props["dbname"] << ")" << std::endl;
   std::cout << "  -L <file>: Initialize the database with the specified Load workload"
             << std::endl;
   std::cout << "  -P <file>: Indicates that the database has been preloaded with "
                "the specified Load workload"
             << std::endl;
   std::cout << "  -W <file>: Perform the Run workload specified in <file>" << std::endl;
   std::cout << "  -p <prop> <val>: set property <prop> to value <val>" << std::endl;
   std::cout << "  -w <prop> <val>: set a property in the previously specified "
                "workload"
             << std::endl;
   std::cout << "Exactly one Load workload is allowed, but multiple Run workloads "
                "may be given."
             << std::endl;
   std::cout << "Run workloads will be executed in the order given on the command "
                "line."
             << std::endl;
}

static void ParseCommandLine(int                         argc,
                           const char                  *argv[],
                           utils::Properties           &props,
                           WorkloadProperties          &load_workload,
                           std::vector<WorkloadProperties> &run_workloads) {
   bool                saw_load_workload = false;
   WorkloadProperties *last_workload     = NULL;
   int                 argindex          = 1;

   load_workload.filename  = "";
   load_workload.preloaded = false;

   props.SetProperty("benchmark", "ycsb");

   for (auto const &[key, val] : default_props) {
      props.SetProperty(key, val);
   }

   while (argindex < argc && StrStartWith(argv[argindex], "-")) {
      if (strcmp(argv[argindex], "-threads") == 0) {
         argindex++;
         if (argindex >= argc) {
            UsageMessage(argv[0]);
            exit(0);
         }
         props.SetProperty("threadcount", argv[argindex]);
         argindex++;
      } else if (strcmp(argv[argindex], "-db") == 0) {
         argindex++;
         if (argindex >= argc) {
            UsageMessage(argv[0]);
            exit(0);
         }
         props.SetProperty("dbname", argv[argindex]);
         argindex++;
      } else if (strcmp(argv[argindex], "-progress") == 0) {
         argindex++;
         if (argindex >= argc) {
            UsageMessage(argv[0]);
            exit(0);
         }
         props.SetProperty("progress", argv[argindex]);
         argindex++;
      } else if (strcmp(argv[argindex], "-host") == 0) {
         argindex++;
         if (argindex >= argc) {
            UsageMessage(argv[0]);
            exit(0);
         }
         props.SetProperty("host", argv[argindex]);
         argindex++;
      } else if (strcmp(argv[argindex], "-port") == 0) {
         argindex++;
         if (argindex >= argc) {
            UsageMessage(argv[0]);
            exit(0);
         }
         props.SetProperty("port", argv[argindex]);
         argindex++;
      } else if (strcmp(argv[argindex], "-slaves") == 0) {
         argindex++;
         if (argindex >= argc) {
            UsageMessage(argv[0]);
            exit(0);
         }
         props.SetProperty("slaves", argv[argindex]);
         argindex++;
      } else if (strcmp(argv[argindex], "-benchmark") == 0) {
         argindex++;
         if (argindex >= argc) {
            UsageMessage(argv[0]);
            exit(0);
         }
         props.SetProperty("benchmark", argv[argindex]);
         argindex++;
      } else if (strcmp(argv[argindex], "-client") == 0) {
         argindex++;
         if (argindex >= argc) {
            UsageMessage(argv[0]);
            exit(0);
         }
         props.SetProperty("client", argv[argindex]);
         argindex++;
      } else if (strcmp(argv[argindex], "-benchmark_seconds") == 0) {
         argindex++;
         if (argindex >= argc) {
            UsageMessage(argv[0]);
            exit(0);
         }
         props.SetProperty("benchmark_seconds", argv[argindex]);
         argindex++;
      } else if (strcmp(argv[argindex], "-W") == 0
                 || strcmp(argv[argindex], "-P") == 0
                 || strcmp(argv[argindex], "-L") == 0)
      {
         WorkloadProperties workload;
         workload.preloaded = strcmp(argv[argindex], "-P") == 0;
         argindex++;
         if (argindex >= argc) {
            UsageMessage(argv[0]);
            exit(0);
         }
         workload.filename.assign(argv[argindex]);
         std::ifstream input(argv[argindex]);
         try {
            workload.props.Load(input);
         } catch (const std::string &message) {
            std::cout << message << std::endl;
            exit(0);
         }
         input.close();
         argindex++;
         if (strcmp(argv[argindex - 2], "-W") == 0) {
            run_workloads.push_back(workload);
            last_workload = &run_workloads[run_workloads.size() - 1];
         } else if (saw_load_workload) {
            UsageMessage(argv[0]);
            exit(0);
         } else {
            saw_load_workload = true;
            load_workload     = workload;
            last_workload     = &load_workload;
         }
      } else if (strcmp(argv[argindex], "-p") == 0
                 || strcmp(argv[argindex], "-w") == 0)
      {
         argindex++;
         if (argindex >= argc) {
            UsageMessage(argv[0]);
            exit(0);
         }
         std::string propkey = argv[argindex];
         argindex++;
         if (argindex >= argc) {
            UsageMessage(argv[0]);
            exit(0);
         }
         std::string propval = argv[argindex];
         if (strcmp(argv[argindex - 2], "-w") == 0) {
            if (last_workload) {
               last_workload->props.SetProperty(propkey, propval);
            } else {
               UsageMessage(argv[0]);
               exit(0);
            }
         } else {
            props.SetProperty(propkey, propval);
         }
         argindex++;
      } else {
         std::cout << "Unknown option '" << argv[argindex] << "'" << std::endl;
         exit(0);
      }
   }

   if (argindex == 1 || argindex != argc
       || (props.GetProperty("benchmark") == "ycsb" && !saw_load_workload))
   {
      UsageMessage(argv[0]);
      exit(0);
   }
}

} // namespace ycsbc 