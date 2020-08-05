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

const std::string kTableName = "Singers";
const std::string kUpdateColumnName = "Age";
const std::string kBaseColumnName = "DefaultAge";

void ReadWriteTransaction(google::cloud::spanner::Client client) {
  namespace spanner = ::google::cloud::spanner;
  using ::google::cloud::StatusOr;

  // A helper to read a single Singers DefaultAge.
  auto get_default_age =
      [](spanner::Client client, spanner::Transaction txn,
         std::int64_t id) -> StatusOr<std::int64_t> {
    auto key = spanner::KeySet().AddKey(spanner::MakeKey(id));
    auto rows = client.Read(std::move(txn), "Singers", std::move(key),
                            {"DefaultAge"});
    using RowType = std::tuple<std::int64_t>;
    auto row = spanner::GetSingularRow(spanner::StreamOf<RowType>(rows));
    if (!row) return std::move(row).status();
    return std::get<0>(*std::move(row));
  };

  // A helper to read a single Singers Age
  auto get_current_age =
      [](spanner::Client client, spanner::Transaction txn,
         std::int64_t id) -> StatusOr<std::int64_t> {
    auto key = spanner::KeySet().AddKey(spanner::MakeKey(id));
    auto rows = client.Read(std::move(txn), "Singers", std::move(key),
                            {"Age"});
    using RowType = std::tuple<std::int64_t>;
    auto row = spanner::GetSingularRow(spanner::StreamOf<RowType>(rows));
    if (!row) return std::move(row).status();
    return std::get<0>(*std::move(row));
  };

  auto commit = client.Commit(
      [&client, &get_default_age, &get_current_age](
          spanner::Transaction const& txn) -> StatusOr<spanner::Mutations> {
        std::int64_t id = 1;
        auto defaultAge = get_default_age(client, txn, id);
        if (!defaultAge) return std::move(defaultAge).status();
        auto age = get_current_age(client, txn, id);
	
	if(age) {
             std::int64_t ageGap = *age - *defaultAge;
	     if(ageGap != 100) {
	     	return google::cloud::Status(
		    google::cloud::StatusCode::kUnknown,
		    "Age gap is wrong for Singer ID " + std::to_string(id)); 
	     }
	     std::cout << "No need to update for Singer ID " + std::to_string(id) + "\n";
	     return spanner::Mutations{};
	}
	std::cout << "Update Age for Singer ID " + std::to_string(id) + "\n";
        std::int64_t ageGap = 100;
        return spanner::Mutations{
            spanner::UpdateMutationBuilder(
                "Singers", {"SingerId", "Age"})
                .EmplaceRow(id, *defaultAge + ageGap)
                .Build()};
      });

  if (!commit) throw std::runtime_error(commit.status().message());
  
  std::cout << "Update was successful [spanner_read_write_transaction]\n";
}

void InsertData(google::cloud::spanner::Client client, std::int64_t singerId, std::int64_t albumId) {
  namespace spanner = ::google::cloud::spanner;
  auto insert_albums = spanner::InsertMutationBuilder(
                           "Albums", {"SingerId", "AlbumId"})
                           .EmplaceRow(singerId, albumId)
                           .Build();
  auto commit_result =
      client.Commit(spanner::Mutations{insert_albums});
  if (!commit_result) {
    throw std::runtime_error(commit_result.status().message());
  }
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
 
  auto start = std::chrono::high_resolution_clock::now();
  
  //ReadWriteTransaction(client);
  std::int64_t singerId = 1;
  std::int64_t albumId = 1;
  for(int i = 0; i < 10000; i++) {
     InsertData(client, singerId, albumId);
     singerId ++;
     albumId ++;
  }
  auto stop = std::chrono::high_resolution_clock::now();
  auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(stop-start);
  std::cout << "Time taken for a read-write transaction is " << duration.count() 
	  << " milliseconds" << std::endl;
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
