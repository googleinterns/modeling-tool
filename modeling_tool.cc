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

#include "google/cloud/spanner/client.h"
#include <iostream>
#include <stdexcept>
#include <chrono>

namespace {
namespace spanner = ::google::cloud::spanner;
using google::cloud::StatusOr;

const std::int64_t DAYINTERVAL = 60; 
const std::int64_t BATCHSIZE = 1000; // mutations per commit 
const std::string TABLE = "TestModels";

int batchUpdateData(spanner::Client readClient, spanner::Client writeClient,
           std::int64_t batchSize)  {
  auto rows = readClient.Read(TABLE, spanner::KeySet::All(),
  		{"CdsId", "ExpirationTime", "TrainingTime"});
  int updatedRecord = 0; 
  spanner::Mutations mutations;
  std::int64_t i = 0;
  for(const auto& row : rows) {
    if(!row) throw std::runtime_error(row.status().message());
    
    spanner::Value cds = (*row).get(0).value();
    spanner::Value expiration = (*row).get(1).value();
    spanner::Value training = (*row).get(2).value();

    std::int64_t cdsId = cds.get<std::int64_t>().value();
    StatusOr<spanner::Timestamp> expirationTime = expiration.get<spanner::Timestamp>();
    StatusOr<spanner::Timestamp> trainingTime = training.get<spanner::Timestamp>();
    
    if(!trainingTime) {
      throw std::runtime_error("TrainingTime shouldn't be null.");
    }
    if(expirationTime) {
      spanner::sys_time<std::chrono::nanoseconds> trainingNS = 
        (*expirationTime).get<spanner::sys_time<std::chrono::nanoseconds>>().value()
        - DAYINTERVAL*std::chrono::hours(24);
      spanner::Timestamp supposedTraining = spanner::MakeTimestamp(trainingNS).value();
      if(*trainingTime != supposedTraining) {
        throw std::runtime_error("Time gap for " + std::to_string(cdsId) + " is not correct.");
      }
    }
    else {
      spanner::sys_time<std::chrono::nanoseconds> expirationNS = 
        (*trainingTime).get<spanner::sys_time<std::chrono::nanoseconds>>().value()
        + DAYINTERVAL*std::chrono::hours(24);
      spanner::Timestamp newExpiration = spanner::MakeTimestamp(expirationNS).value();
      mutations.push_back(spanner::UpdateMutationBuilder(
		  TABLE, {"CdsId", "ExpirationTime", "TrainingTime"})
		  .EmplaceRow(cdsId, newExpiration, *trainingTime)
		  .Build());
      
      ++i;
      if(i%batchSize == 0) {
    	  auto commit_result = writeClient.Commit(mutations);
    	  if (!commit_result) {
     	    throw std::runtime_error(commit_result.status().message());
   	    }
        updatedRecord += mutations.size();
	      mutations.clear();
	      i = 0;
      }
    }
  }
  if(!mutations.empty()) {
    auto commit_result = writeClient.Commit(mutations);
    if (!commit_result) {
     	throw std::runtime_error(commit_result.status().message());
   	}
    updatedRecord += mutations.size();
  }
  return updatedRecord;
}

void batchInsertData(google::cloud::spanner::Client client, std::int64_t batchSize) {
  namespace spanner = ::google::cloud::spanner;
  using ::google::cloud::StatusOr;

  auto commit_result = client.Commit(
	[&client, &batchSize](
		spanner::Transaction const& txn) -> StatusOr<spanner::Mutations> {
        spanner::Mutations mutations; 
        spanner::sys_time<std::chrono::nanoseconds> trainingNS = std::chrono::system_clock::now(); 
        spanner::Timestamp trainingTime = spanner::MakeTimestamp(trainingNS).value();
        for(std::int64_t i = 3; i <= batchSize; i++) {
            mutations.push_back(spanner::InsertMutationBuilder(
			      "TestModels", {"CdsId", "TrainingTime"})
		        .EmplaceRow(i, trainingTime)
			      .Build());
        }
	      return mutations;
      });
  if (!commit_result) {
    throw std::runtime_error(commit_result.status().message());
  }
}

} // namespace

int main(int argc, char* argv[]) try {
  if (argc != 4) {
    std::cerr << "Usage: " << argv[0]
              << " project-id instance-id database-id\n";
    return 1;
  }
  
  spanner::Client readClient(
      spanner::MakeConnection(spanner::Database(argv[1], argv[2], argv[3])));
  spanner::Client writeClient(
      spanner::MakeConnection(spanner::Database(argv[1], argv[2], argv[3])));
  
  auto start = std::chrono::high_resolution_clock::now();
  
  int updatedRecord = batchUpdateData(readClient, writeClient, BATCHSIZE);
  // batchInsertData(writeClient, BATCHSIZE);
  auto stop = std::chrono::high_resolution_clock::now();
  auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(stop-start);
  std::cout << 
  "Time taken : " << duration.count() << " milliseconds" << std::endl;
  std::cout << 
  "Total updated records : " << updatedRecord << " milliseconds" << std::endl;
  return 1;
  } catch (std::exception const& ex) {
  std::cerr << "Standard exception raised: " << ex.what() << "\n";
  return 1;
  }