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

const std::string kTableName = "Singers";
const std::string kUpdateColumnName = "Age";
const std::string kBaseColumnName = "DefaultAge";
// const std::string kCustomDml = "UPDATE Singers SET Age = DefaultAge+10 WHERE Age IS NULL";
const std::string kCustomDml = "UPDATE TestModels SET ExpirationTime = TIMESTAMP_ADD(TrainingTime, INTERVAL 60 DAY) WHERE ExpirationTime IS NULL";

void DmlPartitionedUpdate(google::cloud::spanner::Client client) { 
  namespace spanner = ::google::cloud::spanner;
  auto result = client.ExecutePartitionedDml(
      spanner::SqlStatement(kCustomDml));
  if (!result) throw std::runtime_error(result.status().message());
  std::cout << "Update was successful [spanner_dml_partitioned_update]\n";
}

void ReadWriteTransaction(google::cloud::spanner::Client client) {
  namespace spanner = ::google::cloud::spanner;
  using ::google::cloud::StatusOr;

  // A helper to read a single album MarketingBudget.
  auto get_current_age =
      [](spanner::Client client, spanner::Transaction txn,
         std::int64_t id) -> StatusOr<std::int64_t> {
    auto key = spanner::KeySet().AddKey(spanner::MakeKey(id));
    auto rows = client.Read(std::move(txn), "Singers", std::move(key),
                            {"DefaultAge"});
    
    //auto rows = client.Read(std::move(txn), "Singers", spanner::KeySet::All(),
      //                      {"DefaultAge"});
    using RowType = std::tuple<std::int64_t>;
    auto row = spanner::GetSingularRow(spanner::StreamOf<RowType>(rows));
    if (!row) return std::move(row).status();
    return std::get<0>(*std::move(row));
  };

  auto commit = client.Commit(
      [&client, &get_current_age](
          spanner::Transaction const& txn) -> StatusOr<spanner::Mutations> {
        auto b1 = get_current_age(client, txn, 1);
        if (!b1) return std::move(b1).status();
        //auto b2 = get_current_age(client, txn, 2, 2);
        //if (!b2) return std::move(b2).status();
        //std::int64_t transfer_amount = 200000;
        std::int64_t age_gap = 100;
        return spanner::Mutations{
            spanner::UpdateMutationBuilder(
                "Singers", {"SingerId", "Age"})
                .EmplaceRow(1, *b1 + age_gap)
                .Build()};
      });

  if (!commit) throw std::runtime_error(commit.status().message());
  std::cout << "Transfer was successful [spanner_read_write_transaction]\n";
}

int main(int argc, char* argv[]) try {
  if (argc != 4) {
    std::cerr << "Usage: " << argv[0]
              << " project-id instance-id database-id\n";
    return 1;
  }

  namespace spanner = ::google::cloud::spanner;
  spanner::Client client(
      spanner::MakeConnection(spanner::Database(argv[1], argv[2], argv[3])));
  // DmlPartitionedUpdate(client);
  ReadWriteTransaction(client);
  return 1;
  
  /* 
  auto rows =
      client.ExecuteQuery(spanner::SqlStatement("SELECT 'Hello World'"));

  for (auto const& row : spanner::StreamOf<std::tuple<std::string>>(rows)) {
    if (!row) throw std::runtime_error(row.status().message());
    std::cout << std::get<0>(*row) << "\n";
  }

  return 0;
  */
  } catch (std::exception const& ex) {
  std::cerr << "Standard exception raised: " << ex.what() << "\n";
  return 1;
}
