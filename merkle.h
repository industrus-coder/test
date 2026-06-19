#pragma once
#include "store.h"
#include <vector>
#include <cstdint>
#include <array>

constexpr size_t MERKLE_SLOTS = 1024;          // leaf count
constexpr size_t MERKLE_TREE_NODES = 2047;     // 2 * MERKLE_SLOTS - 1 (full binary tree)
constexpr size_t MERKLE_TREE_DEPTH = 11;       // levels (0 = leaf, 10 = root)

uint16_t get_slot(const Key& key);

// Full Merkle tree — flat array (standard heap), root at index 0,
// leaves at indices [MERKLE_SLOTS - 1, MERKLE_TREE_NODES - 1].
// Internal node i has children at 2*i+1 and 2*i+2.
using MerkleTree = std::array<uint64_t, MERKLE_TREE_NODES>;

struct MerklePayload {
    uint64_t version;
    MerkleTree tree;
};

MerkleTree compute_merkle_tree();
str serialize_merkle_tree(const MerkleTree& tree, uint64_t version);
MerklePayload deserialize_merkle_tree(strv payload);
std::vector<uint16_t> find_differing_slots(
    const MerkleTree& local,
    const MerkleTree& remote);
str get_slot_entries_payload(uint16_t slot);
