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

#define QUEUE_NAME  "/my_queue"
#define MAX_SIZE    1024
#define MSG_STOP    "exit"

using namespace folly;

using json = nlohmann::json;

class JsonHelperClass {
public:
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

    void process_input_and_queue(const json& inputData, const mqd_t& mq) {
        // process and flatten the whole json input
        json flattenedData;
        flattenJSON(inputData, flattenedData);

        // send the flattened json as a string into the msg queue
        //TODO: handle synchronization
        if(flattenedData != nullptr){
            for(const auto& item: flattenedData){
                std::string res = item.str();
                if(mq_send(mq, res.c_str(), MAX_SIZE, 0) == -1){
                    perror("mq_send failed");
                    exit(1);
                }
            }
        }
        else{
            throw std::runtime_error("Couldn't flatten the data for thread: " + std::this_thread.get_id());
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
            perror("mq_open");
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

    // 3. Chunk data before assigning to thread

    // 4. Create threads and assign data

    //TODO: change number of threads, for now hard coded
    std::<vector<std::thread>>threads;
    for(int i = 1; i <= 10; i++){
        threads.push_back(new std::thread(process_input_and_queue, i));
    }

    // 5. Join threads
    for(auto& t : threads){
        t.join();
    }

    //TODO: closing and unlinking msg_queue
}



