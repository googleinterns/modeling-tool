#include "modeling_tool.h"

#include <gmock/gmock.h>
#include "google/cloud/spanner/client.h"
#include "google/cloud/spanner/mocks/mock_spanner_connection.h"
#include <google/protobuf/text_format.h>

namespace {
using ::testing::_;
using ::testing::Return;
using ::testing::Field;
using google::cloud::StatusOr;
namespace spanner = ::google::cloud::spanner;

class ModelingToolTest : public ::testing::Test {
  protected:
  static void SetUpTestSuite() {
    ASSERT_TRUE(google::protobuf::TextFormat::ParseFromString(kText, &metadata));
    // Create mocks for `spanner::Connection`:
    readConn = std::make_shared<google::cloud::spanner_mocks::MockConnection>();
    writeConn = std::make_shared<google::cloud::spanner_mocks::MockConnection>();
  }

  static const auto constexpr kText = R"pb(
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
  static google::spanner::v1::ResultSetMetadata metadata;
  static const std::int64_t DAYINTERVAL = 60;
  static const std::string TABLE;
  static std::shared_ptr<google::cloud::spanner_mocks::MockConnection> readConn;
  static std::shared_ptr<google::cloud::spanner_mocks::MockConnection> writeConn;
};

google::spanner::v1::ResultSetMetadata ModelingToolTest::metadata;
const std::int64_t ModelingToolTest::DAYINTERVAL;
const std::string ModelingToolTest::TABLE = "TestModels";
std::shared_ptr<google::cloud::spanner_mocks::MockConnection> ModelingToolTest::readConn = nullptr;
std::shared_ptr<google::cloud::spanner_mocks::MockConnection> ModelingToolTest::writeConn = nullptr;

TEST_F(ModelingToolTest, SuccessfulBatchUpdate) {
    // Create a mock object to stream the results of Read
    auto source =
      std::unique_ptr<google::cloud::spanner_mocks::MockResultSetSource>(
          new google::cloud::spanner_mocks::MockResultSetSource);
    // Setup the return type of the Read results
    EXPECT_CALL(*source, Metadata()).WillRepeatedly(Return(metadata));

    // Setup the mock source to return values for Read():
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
    // Setup the connection mock to return the rows previously setup:
    EXPECT_CALL(*readConn, Read(_))
        .WillOnce([&source](spanner::Connection::ReadParams const&)
                    -> spanner::RowStream {
        return spanner::RowStream(std::move(source));
        });
    // Setup the connection mock to return values for Commit():
    spanner::sys_time<std::chrono::nanoseconds> expirationNS = 
        trainingNS + DAYINTERVAL*std::chrono::hours(24);
    spanner::Timestamp newExpiration = spanner::MakeTimestamp(expirationNS).value();
    spanner::Mutations updates;
    updates.push_back(spanner::UpdateMutationBuilder(
		  TABLE, {"CdsId", "ExpirationTime", "TrainingTime"})
		  .EmplaceRow(spanner::Value(1), newExpiration, timestamp)
		  .Build());

    // spanner::Connection::CommitParams params{spanner::Transaction(spanner::Transaction::ReadWriteOptions()), updates};
    EXPECT_CALL(*writeConn, Commit(Field(&spanner::Connection::CommitParams::mutations, updates)))
        .WillRepeatedly([](spanner::Connection::CommitParams const&)
                    -> StatusOr<spanner::CommitResult> {
        spanner::sys_time<std::chrono::nanoseconds> commitNS = std::chrono::system_clock::now();
        spanner::Timestamp commitTimestamp = spanner::MakeTimestamp(commitNS).value();
        spanner::CommitResult res{commitTimestamp};
        return StatusOr<spanner::CommitResult>(res);
        });

    updates.clear();
    updates.push_back(spanner::UpdateMutationBuilder(
		  TABLE, {"CdsId", "ExpirationTime", "TrainingTime"})
		  .EmplaceRow(spanner::Value(2), newExpiration, timestamp)
		  .Build());
    EXPECT_CALL(*writeConn, Commit(Field(&spanner::Connection::CommitParams::mutations, updates)))
        .WillRepeatedly([](spanner::Connection::CommitParams const&)
                    -> StatusOr<spanner::CommitResult> {
        spanner::sys_time<std::chrono::nanoseconds> commitNS = std::chrono::system_clock::now();
        spanner::Timestamp commitTimestamp = spanner::MakeTimestamp(commitNS).value();
        spanner::CommitResult res{commitTimestamp};
        return StatusOr<spanner::CommitResult>(res);
        });
    
    // Create clients with the mocked connection:
    spanner::Client readClient(readConn);
    spanner::Client writeClient(writeConn);
    // Should update all records
    EXPECT_EQ(2, modelingtool::batchUpdateData(readClient, writeClient, 1));
}

TEST_F(ModelingToolTest, NoUpdateWhenFieldCheckPassed) {
    // Setup a new mock source to return values for Read():
    auto sourceNotNull =
      std::unique_ptr<google::cloud::spanner_mocks::MockResultSetSource>(
        new google::cloud::spanner_mocks::MockResultSetSource);
    EXPECT_CALL(*sourceNotNull, Metadata()).WillRepeatedly(Return(metadata));

    spanner::sys_time<std::chrono::nanoseconds> trainingNS = std::chrono::system_clock::now();
    spanner::Timestamp timestamp = spanner::MakeTimestamp(trainingNS).value();  
    spanner::sys_time<std::chrono::nanoseconds> expirationNS = 
        trainingNS + DAYINTERVAL*std::chrono::hours(24);
    spanner::Timestamp expiration = spanner::MakeTimestamp(expirationNS).value();    
    EXPECT_CALL(*sourceNotNull, NextRow())
        .WillOnce(Return(
          spanner::MakeTestRow({{"CdsId", spanner::Value(1)},
                                {"ExpirationTime", spanner::Value(expiration)},
                                {"TrainingTime", spanner::Value(timestamp)}})))
        .WillOnce(Return(
          spanner::MakeTestRow({{"CdsId", spanner::Value(2)},
                                {"ExpirationTime", spanner::Value(expiration)},
                                {"TrainingTime", spanner::Value(timestamp)}})))
        .WillOnce(Return(spanner::Row()));
    // Setup the connection mock to return the rows setup above:
    EXPECT_CALL(*readConn, Read(_))
        .WillOnce([&sourceNotNull](spanner::Connection::ReadParams const&)
                    -> spanner::RowStream {
        return spanner::RowStream(std::move(sourceNotNull));
        });
    // Create clients with the mocked connection:
    spanner::Client readClient(readConn);
    spanner::Client writeClient(writeConn);
    // Should not update any records
    EXPECT_EQ(0, modelingtool::batchUpdateData(readClient, writeClient, 1));
}

TEST_F(ModelingToolTest, ThrowErrorWhenTimeGapWrong) {
    // Setup a new mock source to return values for Read():
    auto sourceIncorrect =
      std::unique_ptr<google::cloud::spanner_mocks::MockResultSetSource>(
        new google::cloud::spanner_mocks::MockResultSetSource);
    EXPECT_CALL(*sourceIncorrect, Metadata()).WillRepeatedly(Return(metadata));   
    spanner::sys_time<std::chrono::nanoseconds> trainingNS = std::chrono::system_clock::now();
    spanner::Timestamp timestamp = spanner::MakeTimestamp(trainingNS).value();    
    EXPECT_CALL(*sourceIncorrect, NextRow())
        .WillOnce(Return(
          spanner::MakeTestRow({{"CdsId", spanner::Value(1)},
                                {"ExpirationTime", spanner::Value(timestamp)},
                                {"TrainingTime", spanner::Value(timestamp)}})));
    // Setup the connection mock to return the rows setup above:
    EXPECT_CALL(*readConn, Read(_))
        .WillOnce([&sourceIncorrect](spanner::Connection::ReadParams const&)
                    -> spanner::RowStream {
        return spanner::RowStream(std::move(sourceIncorrect));
        });
    // Should throw std::runtime_error
    try {
        // Create clients with the mocked connection:
        spanner::Client readClient(readConn);
        spanner::Client writeClient(writeConn);
        modelingtool::batchUpdateData(readClient, writeClient, 1);
        FAIL() << "Expected std::runtime_error";
    }
    catch(std::runtime_error const & err) {
        EXPECT_EQ(err.what(), std::string("Time gap for 1 is not correct."));
    }
    catch(...) {
      FAIL() << "Expected std::runtime_error";
    }
}

TEST_F(ModelingToolTest, ThrowErrorWhenRequiredFieldIsNull) {
    // Setup a new mock source to return values for Read():
    auto sourceNull =
      std::unique_ptr<google::cloud::spanner_mocks::MockResultSetSource>(
        new google::cloud::spanner_mocks::MockResultSetSource);
    EXPECT_CALL(*sourceNull, Metadata()).WillRepeatedly(Return(metadata));
    spanner::sys_time<std::chrono::nanoseconds> trainingNS = std::chrono::system_clock::now();
    spanner::Timestamp timestamp = spanner::MakeTimestamp(trainingNS).value();         
    EXPECT_CALL(*sourceNull, NextRow())
        .WillOnce(Return(
          spanner::MakeTestRow({{"CdsId", spanner::Value(1)},
                                {"ExpirationTime", spanner::Value(timestamp)},
                                {"TrainingTime", spanner::MakeNullValue<spanner::Timestamp>()}})));
    // Setup the connection mock to return the rows setup above:
    EXPECT_CALL(*readConn, Read(_))
        .WillOnce([&sourceNull](spanner::Connection::ReadParams const&)
                    -> spanner::RowStream {
        return spanner::RowStream(std::move(sourceNull));
        });
    // Should throw std::runtime_error
    try {
        // Create clients with the mocked connection:
        spanner::Client readClient(readConn);
        spanner::Client writeClient(writeConn);
        modelingtool::batchUpdateData(readClient, writeClient, 1);
        FAIL() << "Expected std::runtime_error";
    }
    catch(std::runtime_error const & err) {
        EXPECT_EQ(err.what(), std::string("TrainingTime shouldn't be null."));
    }
    catch(...) {
      FAIL() << "Expected std::runtime_error";
    }
}
}  // namespace