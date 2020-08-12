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

const std::int64_t DEFAULTGAP = 100;
const std::int64_t BATCHSIZE = 1000;

void batchUpdateData(spanner::Client readClient, spanner::Client writeClient,
           std::int64_t batchSize)  {
  auto rows = readClient.Read("TestModels", spanner::KeySet::All(),
  		{"CdsId", "TrainingTime"});
  using RowType = std::tuple<std::int64_t, spanner::Timestamp>;

  // A helper to read a single Timestamp.
  const auto& get_expiration_time =
      [](spanner::Client client, std::int64_t cdsId) 
      -> StatusOr<spanner::Timestamp> {
    auto key = spanner::KeySet().AddKey(spanner::MakeKey(cdsId));
    auto rows = client.Read("TestModels", std::move(key),
                            {"ExpirationTime"});
    using RowType = std::tuple<spanner::Timestamp>;
    auto row = spanner::GetSingularRow(spanner::StreamOf<RowType>(rows));
    if (!row) return std::move(row).status();
    return std::get<0>(*std::move(row));
  };  

  spanner::Mutations mutations;
  std::int64_t i = 0;
  for(const auto& row : spanner::StreamOf<RowType>(rows)) {
    if(!row) throw std::runtime_error(row.status().message());
    i += 1;
    std::int64_t cdsId = std::get<0>(*row);
    spanner::Timestamp trainingTime = std::get<1>(*row);
    // auto expirationTime = get_expiration_time(readClient, cdsId);
    // if(!expirationTime) {
      spanner::sys_time<std::chrono::nanoseconds> expirationNS = 
        trainingTime.get<spanner::sys_time<std::chrono::nanoseconds>>().value()
        + 60*std::chrono::hours(24);
      spanner::Timestamp newExpirationTime = spanner::MakeTimestamp(expirationNS).value();
      mutations.push_back(spanner::UpdateMutationBuilder(
		  "TestModels", {"CdsId", "ExpirationTime", "TrainingTime"})
		  .EmplaceRow(cdsId, newExpirationTime, trainingTime)
		  .Build());

      if(i % batchSize == 0) {
    	  auto commit_result = writeClient.Commit(mutations);
    	  if (!commit_result) {
     	    throw std::runtime_error(commit_result.status().message());
   	    }
	      mutations.clear();
	      i = 0;
      }
    // }
    // else: check validity
  }
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
  
  batchUpdateData(readClient, writeClient, BATCHSIZE);
  // batchInsertData(writeClient, BATCHSIZE);
  auto stop = std::chrono::high_resolution_clock::now();
  auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(stop-start);
  std::cout << 
  "Time taken : " << duration.count() << " milliseconds" << std::endl;
  return 1;
  } catch (std::exception const& ex) {
  std::cerr << "Standard exception raised: " << ex.what() << "\n";
  return 1;
  }