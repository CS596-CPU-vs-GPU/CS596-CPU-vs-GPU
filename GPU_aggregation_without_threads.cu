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
    mqd_t mq;
    mq = mq_open(QUEUE_NAME, O_RDONLY);
    if (mq == (mqd_t)-1) {
        perror("mq_open");
        exit(1);
    }

    std::vector<size_t> local_buffers;
    std::unordered_map<size_t, char*> hashKeyMapping;
    auto start_reading = std::chrono::high_resolution_clock::now(); 

    while (true) {
        char msg[1024];
        ssize_t bytes_read = mq_receive(mq, (char*)&msg, 1024, NULL);
        std::string message(msg, bytes_read);
        if (bytes_read >= 0) {
            size_t hashKey = MurmurHash3_32(msg, strlen(msg), 0);
            hashKeyMapping[hashKey % HASH_SIZE] = strdup(msg); // Note: strdup allocates memory
            local_buffers.push_back(hashKey);
        } else {
            std::cerr << "Error receiving message: " << strerror(errno) << std::endl;
            continue;
        }
    }

    auto end_reading = std::chrono::high_resolution_clock::now();  // End timing reading and inserting
    std::chrono::duration<double> elapsed_reading = end_reading - start_reading;  // Calculate elapsed time
    std::cout << "Time taken for aggregation: " << elapsed_reading.count() << " seconds." << std::endl;

    file.close();

    // Output the results
    for (const auto& [key, values] : local_buffers) {
        std::cout << "Hash: " << key << std::endl;
        for (const auto& value : values) {
            std::cout << " - " << value << std::endl;
        }
    }

    return 0;
}
