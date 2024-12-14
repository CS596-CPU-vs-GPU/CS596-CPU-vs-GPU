//
// Created by Ananya Reddy on 11/24/2024.
//
#include <iostream>
#include <unordered_map>
#include <unordered_set>
#include <filesystem>
#include <nlohmann/json.hpp>
#include <sstream>
#include <fstream>
#include <fcntl.h>       /* For O_* constants */
#include <sys/stat.h>    /* For mode constants */
#include <mqueue.h>      /* For message queue functions */
#include <unistd.h>
#include <mutex>
#include <chrono>        /* For time counting */

#define QUEUE_NAME  "/my_queue"
#define MAX_SIZE    1024
#define MSG_STOP    "exit"

std::mutex mq_mutex;

using namespace folly;
using json = nlohmann::json;
using namespace std::chrono;

class JsonHelperClass {
public:
    std::vector<json> chunkJSONArray(const json& inputData, int numChunks) {
        std::vector<json> jsonChunks;
        size_t totalSize = inputData.size();
        size_t chunkSize = totalSize / numChunks;

        size_t start = 0;
        while (start < totalSize) {
            size_t end = (start + chunkSize < totalSize) ? (start + chunkSize) : totalSize;
            json chunk = json::array();
            for (size_t i = start; i < end; i++) {
                chunk.push_back(inputData[i]);
            }
            jsonChunks.push_back(chunk);
            start = end;
        }
        return jsonChunks;
    }

    void flattenJSON(const json& input, json& output, const std::string& prefix = "") {
        for (auto it = input.begin(); it != input.end(); ++it) {
            if (it->is_structured()) {
                flattenJSON(*it, output, prefix + it.key() + ".");
            } else {
                output[prefix + it.key()] = *it;
            }
        }
    }

    void process_input_and_queue(const json& inputData, const mqd_t& mq, int& thread_id) {
        std::cout << "Processing records..." << std::endl;
        json flattenedData;
        flattenJSON(inputData, flattenedData);

        if (!flattenedData.empty()) {
            std::lock_guard<std::mutex> lock(mq_mutex);
            for (const auto& item : flattenedData) {
                std::string res = item.dump();
                if (mq_send(mq, res.c_str(), MAX_SIZE, 0) == -1) {
                    perror("mq_send failed");
                    exit(1);
                }
            }
        } else {
            throw std::runtime_error("Failed to flatten data.");
        }
    }

    mqd_t create_message_queue() {
        struct mq_attr attr;
        attr.mq_flags = 0;
        attr.mq_maxmsg = 10;
        attr.mq_msgsize = MAX_SIZE;
        attr.mq_curmsgs = 0;

        mqd_t mq = mq_open(QUEUE_NAME, O_CREAT | O_WRONLY, 0644, &attr);
        if (mq == (mqd_t)-1) {
            perror("Couldn't create a message queue");
            exit(1);
        }
        return mq;
    }

    const mqd_t& get_queue() {
        static const mqd_t& mq = create_message_queue();
        return mq;
    }
};

int main(int argc, char* argv[]) {
    try {
        std::cout << "Main Function in CPU started" << std::endl;

        JsonHelperClass jsonHelper;
        mqd_t mq = jsonHelper.get_queue();

        // 1. Read the input JSON file
        std::string filePath = ""; // Provide path to JSON file
        std::ifstream inputFile(filePath);
        if (!inputFile.is_open()) {
            throw std::runtime_error("Unable to open file: " + filePath);
        }

        json inputData;
        inputFile >> inputData;
        inputFile.close();

        if (!inputData.is_array() && inputData.is_object()) {
            std::cout << "Input JSON is a single object; wrapping it into an array." << std::endl;
            inputData = json::array({inputData});
        }

        // 2. Measure time taken for processing records
        auto startTime = high_resolution_clock::now();

        for (size_t i = 0; i < inputData.size(); ++i) {
            std::cout << "Processing record: " << i + 1 << "/" << inputData.size() << std::endl;

            auto recordStartTime = high_resolution_clock::now();

            json flattenedData;
            jsonHelper.flattenJSON(inputData[i], flattenedData);

            std::string res = flattenedData.dump();
            if (mq_send(mq, res.c_str(), MAX_SIZE, 0) == -1) {
                perror("mq_send failed");
                exit(1);
            }

            auto recordEndTime = high_resolution_clock::now();
            auto recordDuration = duration_cast<milliseconds>(recordEndTime - recordStartTime);
            std::cout << "Record " << i + 1 << " processed in: " << recordDuration.count() << " ms" << std::endl;
        }

        auto endTime = high_resolution_clock::now();
        auto totalDuration = duration_cast<seconds>(endTime - startTime);
        std::cout << "Total time taken for processing all records: " << totalDuration.count() << " seconds"
                  << std::endl;

        // Clean up the message queue
        if (mq_close(mq) == -1) {
            perror("mq_close failed");
        }
        if (mq_unlink(QUEUE_NAME) == -1) {
            perror("mq_unlink failed");
        }

        return 0;
    }
    catch (const std::exception &e) {
        std::cerr << "Error: " << e.what() << std::endl;
        return 1;
    }
}