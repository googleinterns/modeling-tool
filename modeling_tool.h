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
#ifndef MODELING_TOOL_H
#define MODELING_TOOL_H

#include <chrono>
#include <iostream>
#include <stdexcept>

#include "google/cloud/spanner/client.h"

namespace modeling_tool{
namespace spanner = ::google::cloud::spanner;
using ::google::cloud::StatusOr;

const char* COLUMNS[] = {"CdsId",  "ExpirationTime", "TrainingTime"};
const std::int64_t DAYINTERVAL = 60; 
const std::string TABLE = "TestModels";

StatusOr<std::int64_t> batchUpdateData(spanner::Client& readClient, 
                                       spanner::Client& writeClient, std::int64_t batchSize)  {
  std::vector<std::string> columnNames;
  for(const auto *column : COLUMNS) {
    columnNames.push_back(std::string(column));
  }                                       
  auto rows = readClient.Read(TABLE, spanner::KeySet::All(), columnNames);
  int updatedRecord = 0; 
  spanner::Mutations mutations;
  std::int64_t i = 0;
  for(const auto& row : rows) {
    if(!row) return row.status();
    
    spanner::Value cds = row->get(0).value();
    spanner::Value expiration = row->get(1).value();
    spanner::Value training = row->get(2).value();

    std::int64_t cdsId = cds.get<std::int64_t>().value();
    StatusOr<spanner::Timestamp> expirationTime = expiration.get<spanner::Timestamp>();
    StatusOr<spanner::Timestamp> trainingTime = training.get<spanner::Timestamp>();
    if(!trainingTime) return google::cloud::Status(
                        google::cloud::StatusCode::kFailedPrecondition,
                        "TrainingTime shouldn't be null.");
    if(expirationTime) {
      spanner::sys_time<std::chrono::nanoseconds> trainingNS = 
        (*expirationTime).get<spanner::sys_time<std::chrono::nanoseconds>>().value()
        - DAYINTERVAL*std::chrono::hours(24);
      spanner::Timestamp supposedTraining = spanner::MakeTimestamp(trainingNS).value();
      if(*trainingTime != supposedTraining) return google::cloud::Status(
                                              google::cloud::StatusCode::kFailedPrecondition,
                                              "Time gap for " + std::to_string(cdsId) + " is not correct.");
    }
    else {
      spanner::sys_time<std::chrono::nanoseconds> expirationNS = 
        (*trainingTime).get<spanner::sys_time<std::chrono::nanoseconds>>().value()
        + DAYINTERVAL*std::chrono::hours(24);
      spanner::Timestamp newExpiration = spanner::MakeTimestamp(expirationNS).value();
      mutations.push_back(spanner::UpdateMutationBuilder(TABLE, columnNames)
		  .EmplaceRow(cdsId, newExpiration, *trainingTime)
		  .Build());
      ++i;
      if(i%batchSize == 0) {
          writeClient.Commit(mutations);
    	  const auto& commitResult = writeClient.Commit(mutations);
    	  if (!commitResult) return commitResult.status();
        updatedRecord += mutations.size();
	      mutations.clear();
	      i = 0;
      }
    }
  }
  if(!mutations.empty()) {
    const auto& commitResult = writeClient.Commit(mutations);
    if (!commitResult) return commitResult.status();
    updatedRecord += mutations.size();
  }
  return StatusOr<std::int64_t>(updatedRecord);
}

google::cloud::Status batchInsertData(spanner::Client& client, std::int64_t batchSize) {
  std::vector<std::string> columnNames;
  for(const auto *column : COLUMNS) {
    columnNames.push_back(std::string(column));
  }    
  const auto& commitResult = client.Commit(
	[&client, &batchSize, &columnNames](
		spanner::Transaction const& txn) -> StatusOr<spanner::Mutations> {
        spanner::Mutations mutations; 
        spanner::sys_time<std::chrono::nanoseconds> trainingNS = std::chrono::system_clock::now(); 
        spanner::Timestamp trainingTime = spanner::MakeTimestamp(trainingNS).value();
        for(std::int64_t i = 0; i <= batchSize; i++) {
            mutations.push_back(spanner::InsertMutationBuilder(TABLE, columnNames)
		        .EmplaceRow(i, trainingTime)
			      .Build());
        }
	      return mutations;
      });
  return commitResult.status();
}
} // namespace modeling_tool

#endif // MODELING_TOOL_H