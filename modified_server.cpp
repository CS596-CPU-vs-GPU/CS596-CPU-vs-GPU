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

	json read_and_process_file(const std::string& filePath) {
	    std::ifstream inputFile(filePath);
	    if (!inputFile.is_open()) {
	        throw std::runtime_error("Unable to open file: " + filePath);
	    }

	    json inputData;
	    inputFile >> inputData; // Parse the JSON from file
	    inputFile.close();

	    json flattenedData;
	    flattenJSON(inputData, flattenedData);
	    return flattenedData;
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
	cout << "Function C++ started" << endl;

	JsonHelperClass jsonHelper;
	mqd_t mq = jsonHelper.get_queue();
	
	json flattenedData = read_and_process_file("FilePath");
	if(flattenedData != nullptr)
	{
		for (const auto& item : flattenedData) {
			std::string result = item.str();
			if (mq_send(mq, result.c_str(), MAX_SIZE, 0) == -1) {
				perror("mq_send");
				exit(1);
			}
		}
	}
	else
	{
		cout << "Exception in processing" << endl;
		return 0;
	}
} catch(const std::exception& e) {
	std::cerr << "UnHandled Exception Caught im main() ex:" << e.what() << '\n';
	return 0;
}


