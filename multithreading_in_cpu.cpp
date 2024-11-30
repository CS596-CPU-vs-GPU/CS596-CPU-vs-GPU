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

#define QUEUE_NAME  "/my_queue"
#define MAX_SIZE    1024
#define MSG_STOP    "exit"

std::mutex mq_mutex;

using namespace folly;

using json = nlohmann::json;

//TODO: change the access modifiers
class JsonHelperClass {
public:
    // Chunking an list of dictionaries and return a vector with each chunk
    std::vector<json> chunkJSONArray(const json& inputData, int numChunks){
        std::vector<json>> jsonChunks;
        size_t totalSize = inputData.size();
        size_t chunkSize = totalSize / numChunks; // no. of elements in each chunk

        size_t start = 0;
        while(start < totalSize){
            size_t end = (start + chunkSize < totalSize) ? (start + chunkSize) : totalSize;
            json chunk = json::array();
            for(size_t i = start; i < end; i++){
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

    void process_input_and_queue(const json& inputData, const mqd_t& mq, int& thread_id) {
        cout << "Thread " + thread_id + "is running..." << endl;
        // process and flatten the whole json input
        json flattenedData;
        flattenJSON(inputData, flattenedData);

        // send the flattened json as a string into the msg queue
        if(flattenedData != nullptr){
            //Critical section: lock mutex
            std::lock_guard<std::mutex> lock(mq_mutex);
            for(const auto& item: flattenedData){
                std::string res = item.str();
                if(mq_send(mq, res.c_str(), MAX_SIZE, 0) == -1){
                    perror("mq_send failed for thread " + std::to_string(thread_id));
                    exit(1);
                }
            }
        }
        else{
            throw std::runtime_error("Couldn't flatten the data for thread: " + std::to_string(thread_id));
        }

    }

    mqd_t create_message_queue(){
        struct mq_attr attr;

        // Set message queue attributes
        attr.mq_flags = 0;        // Blocking mode
        attr.mq_maxmsg = 10;      // Maximum number of messages in the queue
        attr.mq_msgsize = MAX_SIZE;  // Maximum message size
        attr.mq_curmsgs = 0;      // Number of messages currently in the queue

        // Create or open the message queue
        mqd_t mq = mq_open(QUEUE_NAME, O_CREAT | O_WRONLY, 0644, &attr);
        if (mq == (mqd_t)-1) {
            perror("Couldn't create a msg queue");
            exit(1);
        }
        return mq;
    }

    const mqd_t& get_queue(){
        static const mqd_t& mq = create_message_queue();
        return mq;
    }
};

int main(int argc, char *argv[]) try {
    cout << "Main Function in CPU started" << endl;

    JsonHelperClass jsonHelper;
    mqd_t mq = jsonHelper.get_queue();

    // 1.Reading the input json string file into ifstream object
    string filePath = ""; //TODO: give filePath via CLI
    std::ifstream inputFile(filePath)
    if(!inputFile.is_open()){
        throw std::runtime_error("Unable to open file: " + filePath);
    }

    // 2. Parsing the file into a JSON object
    json inputData;
    inputFile >> inputData;
    inputFile.close();

    //Check for inputData being a json array
    if(!inputData.is_array() && inputData.is_object()){
        std::cout << "Input JSON is not an array but a single JSON. We are converting wrapping into an array of one element." << std::endl;
        inputData = json::array({inputData});
    }
    //TODO: check for invalid json

    // 3. Divide the JSON array into chunks
    std::vector<json> chunks = chunkJSONArray(inputData, 10); //TODO: remove hardcoded numChunks = numThreads = 10

    // 4. Create threads and assign chunks. Using a logical thread id just for logging purpose
    std::<vector<std::thread>>threads;
    for(int i = 1; i <= 10; i++){ //TODO: change number of threads, for now hard coded.
        //Using emplace_back helps create thread directly in the vector, instead of moving it after creating
        //args are address of functions (ptrs to fns)
        threads.emplace_back(&JsonHelperClass::process_input_and_queue, &jsonHelper, std::ref(chunks[i-1]), std:ref(mq), i);
    }

    // 5. Join threads
    for(auto& t : threads){
        t.join();
    }

    //TODO: closing and unlinking msg_queue

    return 0;
}



