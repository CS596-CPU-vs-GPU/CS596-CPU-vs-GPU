
#include <cuda_runtime.h>
#include <cstring>
#include <iostream>
#include <fstream>
#include <chrono>
#include "constants.h"
#include <functional>
#include <cstdint>
#include <fcntl.h>
#include <mqueue.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <atomic>
#include <thread>
#include <mutex>
#include <nlohmann/json.hpp>

#define QUEUE_NAME  "/my_queue"

int MAX_SIZE  =  1024;
int TIME_INTERVAL = 300;
int LOCAL_BUFFER_SIZE = 256;

std::mutex fileMutex;
using namespace std::chrono; 
using json = nlohmann::json;

void loadConfigParameters(std::string configString){
   json config =  json::parse(configString);
   TIME_INTERVAL = config["constants"]["TIME_INTERVAL"];
   LOCAL_BUFFER_SIZE = config["constants"]["LOCAL_BUFFER_SIZE"];
   MAX_SIZE = config["constants"]["MAX_SIZE"];
}

uint32_t MurmurHash3_32(const void* key, size_t len, uint32_t seed) {
    const uint8_t* data = (const uint8_t*)key;
    const int nblocks = len / 4;

    uint32_t h1 = seed;
    const uint32_t c1 = 0xcc9e2d51;
    const uint32_t c2 = 0x1b873593;

    // Body
    const uint32_t* blocks = (const uint32_t*)(data + nblocks * 4);
    for (int i = -nblocks; i; i++) {
        uint32_t k1 = blocks[i];

        k1 *= c1;
        k1 = (k1 << 15) | (k1 >> (32 - 15));
        k1 *= c2;

        h1 ^= k1;
        h1 = (h1 << 13) | (h1 >> (32 - 13));
        h1 = h1 * 5 + 0xe6546b64;
    }

    // Tail
    const uint8_t* tail = (const uint8_t*)(data + nblocks * 4);
    uint32_t k1 = 0;

    switch (len & 3) {
        case 3:
            k1 ^= tail[2] << 16;
        case 2:
            k1 ^= tail[1] << 8;
        case 1:
            k1 ^= tail[0];
            k1 *= c1;
            k1 = (k1 << 15) | (k1 >> (32 - 15));
            k1 *= c2;
            h1 ^= k1;
    }

    // Finalization
    h1 ^= len;

    h1 ^= h1 >> 16;
    h1 *= 0x85ebca6b;
    h1 ^= h1 >> 13;
    h1 *= 0xc2b2ae35;
    h1 ^= h1 >> 16;

    return h1;
}

__global__ void countOccurrences(uint32_t* data, int* counts, int size) {
    int idx = threadIdx.x + blockIdx.x * blockDim.x;
    if (idx < size) {
        atomicAdd(&counts[data[idx]], 1);
    }
}

void processQueue() {
    mqd_t mq;
    struct mq_attr attr;
    char buffer[MAX_SIZE];
    uint32_t hashBuffer[LOCAL_BUFFER_SIZE];
    int hashCount = 0;

    // Open the message queue
    mq = mq_open(QUEUE_NAME, O_RDONLY);
    if (mq == (mqd_t)-1) {
        std::cerr << "Error opening queue" << std::endl;
        return;
    }

    // Get the attributes of the queue
    mq_getattr(mq, &attr);

    auto start = high_resolution_clock::now();

    // Process messages for a fixed duration
    while (true) {
        ssize_t bytes_read = mq_receive(mq, buffer, MAX_SIZE, nullptr);
        if (bytes_read >= 0) {
            uint32_t hash = MurmurHash3_32(buffer, bytes_read, 42);
            hashBuffer[hashCount++] = hash;

            // If buffer is full, process it
            if (hashCount == LOCAL_BUFFER_SIZE) {
                // Allocate memory on GPU
                uint32_t* d_data;
                int* d_counts;
                int* counts = new int[LOCAL_BUFFER_SIZE]();

                cudaMalloc((void**)&d_data, LOCAL_BUFFER_SIZE * sizeof(uint32_t));
                cudaMalloc((void**)&d_counts, LOCAL_BUFFER_SIZE * sizeof(int));

                cudaMemcpy(d_data, hashBuffer, LOCAL_BUFFER_SIZE * sizeof(uint32_t), cudaMemcpyHostToDevice);
                cudaMemcpy(d_counts, counts, LOCAL_BUFFER_SIZE * sizeof(int), cudaMemcpyHostToDevice);

                // Launch kernel
                countOccurrences<<<(LOCAL_BUFFER_SIZE + 255) / 256, 256>>>(d_data, d_counts, LOCAL_BUFFER_SIZE);

                cudaMemcpy(counts, d_counts, LOCAL_BUFFER_SIZE * sizeof(int), cudaMemcpyDeviceToHost);

                // Write counts to file
                {
                    std::lock_guard<std::mutex> lock(fileMutex);
                    std::ofstream outFile("output.txt", std::ios::app);
                    for (int i = 0; i < LOCAL_BUFFER_SIZE; i++) {
                        if (counts[i] > 0) {
                            outFile << i << " " << counts[i] << std::endl;
                        }
                    }
                }

                // Clean up
                cudaFree(d_data);
                cudaFree(d_counts);
                delete[] counts;

                hashCount = 0; // Reset hash buffer
            }
        }

        auto now = high_resolution_clock::now();
        if (duration_cast<seconds>(now - start).count() >= TIME_INTERVAL) {
            break;
        }
    }

    mq_close(mq);
}

int main() {
    processQueue();
    return 0;
}
