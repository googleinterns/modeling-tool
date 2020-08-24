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


static const std::int64_t BATCHSIZE = 1000; // mutations per commit 

int main(int argc, char* argv[]) try {
  if (argc != 4) {
    std::cerr << "Usage: " << argv[0]
              << " project-id instance-id database-id\n";
    return 1;
  }
  namespace spanner = ::google::cloud::spanner;

  spanner::Client readClient(
      spanner::MakeConnection(spanner::Database(argv[1], argv[2], argv[3])));
  spanner::Client writeClient(
      spanner::MakeConnection(spanner::Database(argv[1], argv[2], argv[3])));
  
  auto start = std::chrono::high_resolution_clock::now();
  
  int updatedRecord = modelingtool::batchUpdateData(readClient, writeClient, BATCHSIZE);
  // batchInsertData(writeClient, BATCHSIZE);
  auto stop = std::chrono::high_resolution_clock::now();
  auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(stop-start);
  std::cout << 
  "Time taken: " << duration.count() << " milliseconds" << std::endl;
  std::cout << 
  "Total updated records : " << updatedRecord << " milliseconds" << std::endl;
  return 1;
  } catch (std::exception const& ex) {
  std::cerr << "Standard exception raised: " << ex.what() << "\n";
  return 1;
  }