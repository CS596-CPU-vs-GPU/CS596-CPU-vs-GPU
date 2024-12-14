#include <iostream>
#include <fstream>
#include <cstring>
#include <unordered_map>
#include <vector>
#include <chrono>

#define HASH_SIZE 256

uint32_t MurmurHash3_32(const void* key, size_t len, uint32_t seed) {
    const uint8_t* data = (const uint8_t*)key;
    const int nblocks = len / 4;

    uint32_t h1 = seed;
    const uint32_t c1 = 0xcc9e2d51;
    const uint32_t c2 = 0x1b873593;

    const uint32_t* blocks = (const uint32_t*)(data + nblocks * 4);
    for (int i = -nblocks; i; i++) {
        uint32_t k1 = blocks[i];
        k1 *= c1;
        k1 = (k1 << 15) | (k1 >> 17);
        k1 *= c2;

        h1 ^= k1;
        h1 = (h1 << 13) | (h1 >> 19);
        h1 = h1 * 5 + 0xe6546b64;
    }

    const uint8_t* tail = data + nblocks * 4;
    uint32_t k1 = 0;
    switch (len & 3) {
    case 3: k1 ^= tail[2] << 16;
    case 2: k1 ^= tail[1] << 8;
    case 1: k1 ^= tail[0];
        k1 *= c1;
        k1 = (k1 << 15) | (k1 >> 17);
        k1 *= c2;
        h1 ^= k1;
    }

    h1 ^= len;
    h1 ^= h1 >> 16;
    h1 *= 0x85ebca6b;
    h1 ^= h1 >> 13;
    h1 *= 0xc2b2ae35;
    h1 ^= h1 >> 16;

    return h1;
}

int main() {
    std::ifstream file("input_datasets.json");
    if (!file.is_open()) {
        std::cerr << "Unable to open file: input_datasets.json" << "\n";
        return 1;
    }

    std::string line;
    std::unordered_map<size_t, std::vector<std::string>> hashTable;
    auto start_reading = std::chrono::high_resolution_clock::now(); 

    while (getline(file, line)) {
        uint32_t hashKey = MurmurHash3_32(line.data(), line.length(), 0);
        hashKey %= HASH_SIZE;  // Ensure the hash key is within bounds of HASH_SIZE
        hashTable[hashKey].push_back(line);
    }

    auto end_reading = std::chrono::high_resolution_clock::now();  // End timing reading and inserting
    std::chrono::duration<double> elapsed_reading = end_reading - start_reading;  // Calculate elapsed time
    std::cout << "Time taken for aggregation: " << elapsed_reading.count() << " seconds." << std::endl;

    file.close();

    // Output the results
    for (const auto& [key, values] : hashTable) {
        std::cout << "Hash: " << key << std::endl;
        for (const auto& value : values) {
            std::cout << " - " << value << std::endl;
        }
    }

    return 0;
}
