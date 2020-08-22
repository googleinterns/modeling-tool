#include "google/cloud/spanner/client.h"
#include "google/cloud/spanner/mocks/mock_spanner_connection.h"
#include <google/protobuf/text_format.h>
#include <gmock/gmock.h>
#include "modeling_tool.h"


namespace {

using ::testing::_;
using ::testing::Return;
namespace spanner = ::google::cloud::spanner;
using google::cloud::StatusOr;

TEST(MockSpannerClient, SuccessfulBatchUpdate) {
    // Create a mock object to stream the results of Read
    auto source =
      std::unique_ptr<google::cloud::spanner_mocks::MockResultSetSource>(
          new google::cloud::spanner_mocks::MockResultSetSource);
    auto source2 =
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
    EXPECT_CALL(*source2, Metadata()).WillRepeatedly(Return(metadata));

    // Setup the mock source to return some values:
    spanner::sys_time<std::chrono::nanoseconds> trainingNS = std::chrono::system_clock::now();
    spanner::Timestamp timestamp = spanner::MakeTimestamp(trainingNS).value();
    EXPECT_CALL(*source, NextRow())
        .WillOnce(Return(
          spanner::MakeTestRow({{"CdsId", spanner::Value(1)},
                                {"ExpirationTime", spanner::MakeNullValue<spanner::Timestamp>()},
                                {"TrainingTime", spanner::Value(timestamp)}})))
        .WillOnce(Return(
          spanner::MakeTestRow({{"CdsId", spanner::Value(2)},
                                {"ExpirationTime", spanner::MakeNullValue<spanner::Timestamp>()},
                                {"TrainingTime", spanner::Value(timestamp)}})))
        .WillOnce(Return(spanner::Row()));

    // Create a mock for `spanner::Connection`:
    auto readConn = std::make_shared<google::cloud::spanner_mocks::MockConnection>();
    // auto writeConn = std::make_shared<google::cloud::spanner_mocks::MockConnection>();
    // Setup the connection mock to return the results previously setup:
    EXPECT_CALL(*readConn, Read(_))
        .WillOnce([&source](spanner::Connection::ReadParams const&)
                    -> spanner::RowStream {
        return spanner::RowStream(std::move(source));
        });
    auto writeConn = std::make_shared<google::cloud::spanner_mocks::MockConnection>();
    EXPECT_CALL(*writeConn, Commit(_))
        .Times(2);
    
    // auto writeConn = std::make_shared<google::cloud::spanner_mocks::MockConnection>();
    // Create clients with the mocked connection:
    spanner::Client readClient(readConn);
    spanner::Client writeClient(writeConn);
    EXPECT_EQ(2, batchUpdateData(readClient, writeClient, 1));
    EXPECT_CALL(*source2, NextRow())
        .WillOnce(Return(
          spanner::MakeTestRow({{"CdsId", spanner::Value(1)},
                                {"ExpirationTime", spanner::MakeNullValue<spanner::Timestamp>()},
                                {"TrainingTime", spanner::Value(timestamp)}})))
        .WillOnce(Return(
          spanner::MakeTestRow({{"CdsId", spanner::Value(2)},
                                {"ExpirationTime", spanner::MakeNullValue<spanner::Timestamp>()},
                                {"TrainingTime", spanner::Value(timestamp)}})))
        .WillOnce(Return(spanner::Row()));
    EXPECT_CALL(*readConn, Read(_))
        .WillOnce([&source2](spanner::Connection::ReadParams const&)
                    -> spanner::RowStream {
        return spanner::RowStream(std::move(source2));
        });
    // Make the request and verify the expected results:
    auto rows = readClient.Read("TestModels", spanner::KeySet::All(),
       {"CdsId", "ExpirationTime", "TrainingTime"});
    int count = 0;
    // using RowType = std::tuple<std::int64_t, spanner::Timestamp, spanner::Timestamp>;
    // for (auto const& row : spanner::StreamOf<RowType>(rows)) {
    
    for (auto const& row : rows) {
        ASSERT_TRUE(row);
        auto expected_id = ++count;
        spanner::Value cds = (*row).get(0).value();
        spanner::Value expiration = (*row).get(1).value();
        spanner::Value training = (*row).get(2).value();
        EXPECT_EQ(expected_id, cds.get<std::int64_t>().value());
        EXPECT_EQ(spanner::MakeNullValue<spanner::Timestamp>(), expiration);
        EXPECT_EQ(spanner::Value(timestamp), training);
    }
    
    
}
}  // namespace