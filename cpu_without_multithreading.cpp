#include <iostream>
#include <unordered_map>
#include <filesystem>
#include <nlohmann/json.hpp>
#include <sstream>
#include <fstream>
#include <fcntl.h>       /* For O_* constants */
#include <sys/stat.h>    /* For mode constants */
#include <mqueue.h>      /* For message queue functions */
#include <unistd.h>
#include <chrono>
#include <mutex>

#define QUEUE_NAME  "/my_queue"
#define MAX_SIZE    1024
#define MSG_STOP    "exit"

std::mutex mq_mutex;

using json = nlohmann::json;
using namespace std;

// JsonHelperClass with necessary methods
class JsonHelperClass {
    public:
        // Chunking an list of dictionaries and return a vector with each chunk
        std::vector<json> chunkJSONArray(const json& inputData, int numChunks) {
            std::vector<json> jsonChunks;
            size_t totalSize = inputData.size();
            size_t chunkSize = totalSize / numChunks; // no. of elements in each chunk

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

        // Function to flatten a nested JSON structure
        void flattenJSON(const json& input, json& output, const std::string& prefix = "") {
            for (auto it = input.begin(); it != input.end(); ++it) {
                if (it->is_structured()) {
                    flattenJSON(*it, output, prefix + it.key() + ".");
                } else {
                    output[prefix + it.key()] = *it;
                }
            }
        }

        // Function to process and queue flattened JSON data
        void process_input_and_queue(const json& inputData, const mqd_t& mq) {
            cout << "Processing JSON data and sending to the message queue..." << endl;

            json flattenedData;
            flattenJSON(inputData, flattenedData);

            if (!flattenedData.empty()) {
                std::lock_guard<std::mutex> lock(mq_mutex);
                for (const auto& item : flattenedData.items()) {
                    std::string res = item.value().dump();  // Serialize JSON to string
                    if (mq_send(mq, res.c_str(), MAX_SIZE, 0) == -1) {
                        perror("mq_send failed");
                        exit(1);
                    }
                }
            } else {
                throw std::runtime_error("Couldn't flatten the JSON data.");
            }
        }

        // Function to create a message queue
        mqd_t create_message_queue() {
            struct mq_attr attr;

            // Set message queue attributes
            attr.mq_flags = 0;         // Blocking mode
            attr.mq_maxmsg = 10;       // Maximum number of messages in the queue
            attr.mq_msgsize = MAX_SIZE; // Maximum message size
            attr.mq_curmsgs = 0;       // Number of messages currently in the queue

            // Create or open the message queue
            mqd_t mq = mq_open(QUEUE_NAME, O_CREAT | O_WRONLY, 0644, &attr);
            if (mq == (mqd_t)-1) {
                perror("Couldn't create a message queue");
                exit(1);
            }
            return mq;
        }

        // Function to get a static reference to the queue
        const mqd_t& get_queue() {
            static const mqd_t& mq = create_message_queue();
            return mq;
        }
};

int main(int argc, char* argv[]) {
    try {
        cout << "Starting sequential JSON processing with message queue..." << endl;

        // Initialize helper class and message queue
        JsonHelperClass jsonHelper;
        mqd_t mq = jsonHelper.get_queue();

        // Reading the input JSON file
        string filePath = "input.json"; // Provide path via CLI or change here
        std::ifstream inputFile(filePath);
        if (!inputFile.is_open()) {
            throw std::runtime_error("Unable to open file: " + filePath);
        }

        // Parse file into JSON object
        json inputData;
        inputFile >> inputData;
        inputFile.close();

        // Ensure inputData is a JSON array
        if (!inputData.is_array() && inputData.is_object()) {
            cout << "Input JSON is not an array; wrapping into an array of one element." << endl;
            inputData = json::array({inputData});
        }

        // Start time tracking
        auto start_time = std::chrono::high_resolution_clock::now();

        // Process entire JSON data sequentially
        jsonHelper.process_input_and_queue(inputData, mq);

        // End time tracking
        auto end_time = std::chrono::high_resolution_clock::now();
        std::chrono::duration<double> execution_time = end_time - start_time;

        cout << "Processing completed successfully!" << endl;
        cout << "Time taken: " << execution_time.count() << " seconds" << endl;

        // Clean up: Close and unlink message queue
        if (mq_close(mq) == -1) {
            perror("mq_close");
            exit(1);
        }
        if (mq_unlink(QUEUE_NAME) == -1) {
            perror("mq_unlink");
            exit(1);
        }

        return 0;
    }
        catch (const std::exception& e) {
        cerr << "Error: " << e.what() << endl;
        return 1;
    }
}
