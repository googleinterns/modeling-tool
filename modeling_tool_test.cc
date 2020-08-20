#include "google/cloud/spanner/client.h"
#include "google/cloud/spanner/mocks/mock_spanner_connection.h"
#include <google/protobuf/text_format.h>
//#include "/home/hanyaoliu/forked/modeling-tool/modeling_tool.h"
#include <gmock/gmock.h>

namespace {

using ::testing::_;
using ::testing::Return;
namespace spanner = ::google::cloud::spanner;

TEST(MockSpannerClient, SuccessfulBatchUpdate) {
    // Create a mock object to stream the results of Read
    auto source =
      std::unique_ptr<google::cloud::spanner_mocks::MockResultSetSource>(
          new google::cloud::spanner_mocks::MockResultSetSource);
    // Setup the return type of the Read results
    auto constexpr kText = R"pb(
    row_type: {
      fields: {
        name: "CdsId",
        type: { code: INT64 }
      }
      fields: {
        name: "ExpirationTime",
        type: { code: TIMESTAMP }
      }
      fields: {
        name: "TrainingTime",
        type: { code: TIMESTAMP }
      }      
    })pb";
    google::spanner::v1::ResultSetMetadata metadata;
    ASSERT_TRUE(google::protobuf::TextFormat::ParseFromString(kText, &metadata));
    EXPECT_CALL(*source, Metadata()).WillRepeatedly(Return(metadata));
    
    // Setup the mock source to return some values:
    spanner::sys_time<std::chrono::nanoseconds> trainingNS = std::chrono::system_clock::now();
    spanner::Timestamp timestamp = spanner::MakeTimestamp(trainingNS).value();
    EXPECT_CALL(*source, NextRow())
        .WillOnce(Return(
          spanner::MakeTestRow({{"CdsId", spanner::Value(1)},
                                {"ExpirationTime", spanner::Value(timestamp)},
                                {"TrainingTime", spanner::Value(timestamp)}})))
        .WillOnce(Return(
          spanner::MakeTestRow({{"CdsId", spanner::Value(2)},
                                {"ExpirationTime", spanner::Value(timestamp)},
                                {"TrainingTime", spanner::Value(timestamp)}})))
        .WillOnce(Return(spanner::Row()));

    // Create a mock for `spanner::Connection`:
    auto readConn = std::make_shared<google::cloud::spanner_mocks::MockConnection>();

    // Setup the connection mock to return the results previously setup:
    EXPECT_CALL(*readConn, Read(_))
        .WillOnce([&source](spanner::Connection::ReadParams const&)
                    -> spanner::RowStream {
        return spanner::RowStream(std::move(source));
        });

    // auto writeConn = std::make_shared<google::cloud::spanner_mocks::MockConnection>();
    // Create clients with the mocked connection:
    spanner::Client readClient(readConn);
    // Make the request and verify the expected results:
    auto rows = readClient.Read("TestModels", spanner::KeySet::All(),
      {"CdsId"});
  	  // {"CdsId", "ExpirationTime", "TrainingTime"});
    int count = 0;
    using RowType = std::tuple<std::int64_t, spanner::Timestamp, spanner::Timestamp>;
    for (auto const& row : spanner::StreamOf<RowType>(rows)) {
        ASSERT_TRUE(row);
        auto expected_id = ++count;
        EXPECT_EQ(expected_id, std::get<0>(*row));
        //EXPECT_EQ(spanner::Value(timestamp), std::get<1>(*row));
        //EXPECT_EQ(spanner::Value(timestamp), std::get<2>(*row));
    }
    
}
}  // namespace



