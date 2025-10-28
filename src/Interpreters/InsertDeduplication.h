#pragma once

#include <functional>
#include <variant>
#include <Processors/Chunk.h>
#include <Core/Block.h>
#include <Interpreters/Context_fwd.h>
#include <Interpreters/StorageIDMaybeEmpty.h>
#include <Core/Block_fwd.h>

#include <Common/Logger.h>
#include <base/defines.h>


namespace DB
{

class InsertDependenciesBuilder;
using InsertDependenciesBuilderConstPtr = std::shared_ptr<const InsertDependenciesBuilder>;

class DeduplicationInfo : public ChunkInfo, public std::enable_shared_from_this<DeduplicationInfo>
{
protected:
    // InsertDependenciesBuilder::createChainForDeduplicationRetry needs access to private members
    friend class InsertDependenciesBuilder;

    explicit DeduplicationInfo(bool async_insert_);
    DeduplicationInfo(const DeduplicationInfo & other) = default;

public:
    using Ptr = std::shared_ptr<DeduplicationInfo>;

    static Ptr create(bool async_insert_);

    ChunkInfo::Ptr merge(const ChunkInfo::Ptr & right) const override;
    Ptr mergeSelf(const Ptr & right) const;

    ChunkInfo::Ptr clone() const override;
    Ptr cloneSelf() const;

    struct FilterResult
    {
        Block filtered_block;
        Ptr deduplication_info;
        size_t removed_count = 0;
    };
    FilterResult filterSelfDuplicate();

    std::vector<std::string> getBlockIds(const std::string & partition_id, bool deduplication_enabled) const;

    size_t getCount() const;
    size_t getRows() const;

    std::pair<std::string, size_t> debug(size_t offset) const;
    std::string debug() const;

    // for sync insert: if user token is empty then by_part_writer token would be calculated later by part writer
    // for async insert: if user token is empty then by_data_hash token would be calculated later
    void setUserToken(const String & token, size_t count);
    void setSourceBlockNumber(size_t block_number);
    void setRootViewID(const StorageIDMaybeEmpty & id);

    /// use for provide deduplication hash for the part from one partition
    void setPartWriterHashForPartition(const std::string & hash, size_t count) const;
    /// use for provide deduplication hash for the chunk with maybe multiple partitions in it
    void setPartWriterHashes(const std::vector<std::string> & partitions_hashes, size_t count) const;
    /// hash from part writer would be used as user token for dependent views if no user token has been set before
    void redefineTokensWithDataHash();

    void setViewID(const StorageID & id);
    void setViewBlockNumber(size_t block_number);
    void rememberPartitionChoise(const std::string & partition_id);

    void setInsertDependencies(InsertDependenciesBuilderConstPtr insert_dependencies_);
    void updateOriginalBlock(const Chunk & chunk, SharedHeader header);

    Block deduplicateBlock(const std::vector<std::string> & existing_block_ids, const std::string & partition_id, ContextPtr context);

    const std::string & getLastPartitionChoice() const;
    const std::vector<StorageIDMaybeEmpty> & getVisitedViews() const;

private:
    UInt128 calculateDataHash(size_t offset) const;

    Ptr cloneSelfFilterImpl() const;
    FilterResult filterOriginalBlock(const std::vector<std::string> & collisions, const String & partition_id);
    FilterResult filterImpl(const std::set<size_t> & fitered_offsets, const Block & block);

    Block goRetry(SharedHeader && header, Chunk && filtered_data, Ptr filtered_info, ContextPtr context);

    std::vector<std::string> getHashesForBlocks(Block & block, String partition_id);

    size_t getTokenBegin(size_t pos) const;
    size_t getTokenEnd(size_t pos) const;
    size_t getTokenRows(size_t pos) const;

    std::unordered_map<std::string, std::vector<size_t>> buildBlockIdToOffsetsMap(const std::string & partition_id) const;

    enum class Level
    {
        SOURCE,
        VIEW,
    };

    LoggerPtr logger = getLogger("DedupInfo");
    const bool is_async_insert = false;

    InsertDependenciesBuilderConstPtr insert_dependencies;

    /// When true, no deduplication is performed
    bool disabled = false;

    mutable Level level = Level::SOURCE;

    struct TokenDefinition
    {
        // there is a difference how block ids are generated from these two types of tokens
        // if by_part_writer is set then it is used as is
        // if by_user is set then block id is calculated as a hash of this string extended with extra tokens
        // When both are empty then data hash is calculated and used as by_user token
        std::string by_user;
        std::string by_part_writer;

        struct Extra
        {
            enum Type
            {
                SOURCE_ID,
                SOURCE_NYMBER,
                VIEW_ID,
                VIEW_NUMBER,
            };

            Type type;

            using Range = std::pair<size_t, size_t>;
            std::variant<Range, String> value_variant;

            bool operator==(const Extra & other) const;
            static Extra asSourceID(const String & id);
            static Extra asSourceNumber(uint64_t number);
            static Extra asViewID(const String & id);
            static Extra asViewNumber(uint64_t number);
            std::string toString() const;
        };

        std::vector<Extra> extra_tokens;

        static TokenDefinition asUserToken(std::string token);

        using HashCalculator = std::function<UInt128 ()>;
        std::string getBlockId(const std::string & partition_id, HashCalculator get_data_hash, Level level_);
        std::string debug() const;
        void addExtraToken(const String & token);
        void setPartToken(std::string token);
        void resetPartToken();
        bool empty() const;
        bool canBeExtended(const TokenDefinition & right) const;
        void doExtend(const TokenDefinition & right);
        bool operator==(const TokenDefinition & other) const;
    };

    void addExtraPart(const TokenDefinition::Extra & extra);

    mutable std::vector<TokenDefinition> tokens;
    std::vector<size_t> offsets; // points to the last row for each offset

    Block original_block;
    StorageIDMaybeEmpty original_block_view_id;

    std::vector<StorageIDMaybeEmpty> visited_views;
    std::string last_partition_choice;
};

}
