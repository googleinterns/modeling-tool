// Copyright 2020 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
#include "modeling_tool.h"

#include <chrono>
#include <iostream>
#include <stdexcept>

#include "google/cloud/spanner/client.h"

namespace spanner = ::google::cloud::spanner;
static const std::int64_t BATCHSIZE = 1000; // mutations per commit 

int main(int argc, char* argv[]) {
  if(argc < 4) {
    std::cerr << "Usage 1: " << argv[0]
              << " project-id instance-id database-id\n";
    std::cerr << "Usage 2: " << argv[0]
              << " project-id instance-id database-id --dry-run \n";              
    return 1;
  }
  bool dryRun = false;
  if(argc == 5) dryRun = true;

  spanner::Client readClient(
      spanner::MakeConnection(spanner::Database(argv[1], argv[2], argv[3])));
  spanner::Client writeClient(
      spanner::MakeConnection(spanner::Database(argv[1], argv[2], argv[3])));
  
  auto start = std::chrono::high_resolution_clock::now();
  
  const auto& updateResult = modeling_tool::batchUpdateData(readClient, writeClient, BATCHSIZE, dryRun);
  if(!updateResult) {
    std::cerr << "Update error: " << updateResult.status().message() << "\n";
    return 1;
  }
  auto stop = std::chrono::high_resolution_clock::now();
  auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(stop-start);
  // execution time
  std::cout << 
  "Time taken: " << duration.count() << " milliseconds" << std::endl;

  // update stats
  std::cout << 
   "Total records read : " << updateResult.value().first << std::endl;
  if(dryRun) {
    std::cout << 
   "Total records that need to be updated : " << updateResult.value().second << std::endl; 
  }
  else {
    std::cout << 
   "Total updated records : " << updateResult.value().second << std::endl;
  }
  
  return 0;
  } 