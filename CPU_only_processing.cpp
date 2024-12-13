#include <iostream>
#include <unordered_map>
#include <fstream>
#include <nlohmann/json.hpp>
#include <cstring>

using json = nlohmann::json;

// MurmurHash3 implementation
uint32_t murmurHash3(const std::string& key) {
    uint32_t seed = 42; // Arbitrary seed value
    const uint32_t m = 0x5bd1e995;
    const int r = 24;

    uint32_t len = key.length();
    uint32_t h = seed ^ len;

    const unsigned char* data = reinterpret_cast<const unsigned char*>(key.data());
    while (len >= 4) {
        uint32_t k = *(reinterpret_cast<const uint32_t*>(data));

        k *= m;
        k ^= k >> r;
        k *= m;

        h *= m;
        h ^= k;

        data += 4;
        len -= 4;
    }

    switch (len) {
        case 3: h ^= data[2] << 16;
        case 2: h ^= data[1] << 8;
        case 1: h ^= data[0];
            h *= m;
    }

    h ^= h >> 13;
    h *= m;
    h ^= h >> 15;

    return h;
}

// Custom hash function for unordered_map
struct MurmurHash {
    size_t operator()(const std::string& key) const {
        return murmurHash3(key);
    }
};

class JsonProcessor {
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

    // Function to process the flattened JSON and perform aggregation using MurmurHash
    void processFlattenedJSON(const json& flattenedData, std::unordered_map<std::string, int, MurmurHash>& aggregationMap) {
        for (auto it = flattenedData.begin(); it != flattenedData.end(); ++it) {
            // Extract the value as a string
            std::string value = it.value().dump(); // Serialize the value to string

            // Increment count in the hash map
            aggregationMap[value]++;
        }
    }
};

int main(int argc, char* argv[]) {
//    try {
        std::cout << "Starting CPU-only JSON processing with MurmurHash..." << std::endl;

        JsonProcessor jsonProcessor;

        // File path input (hardcoded for now; replace with CLI argument if needed)
        std::string filePath = "input.json"; // TODO: Update with actual file path or accept via CLI

        // Open the JSON file
        std::ifstream inputFile(filePath);
        if (!inputFile.is_open()) {
            throw std::runtime_error("Unable to open file: " + filePath);
        }

        // Parse the file into a JSON object
        json inputData;
        inputFile >> inputData;
        inputFile.close();

        // Check if input is a JSON array; if not, wrap it in an array
        if (!inputData.is_array() && inputData.is_object()) {
            std::cout << "Input JSON is not an array. Wrapping it into an array of one element." << std::endl;
            inputData = json::array({inputData});
        }

        // Flatten the entire JSON structure
        json flattenedData;
        for (const auto& item : inputData) {
            json flattenedItem;
            jsonProcessor.flattenJSON(item, flattenedItem);
            for (auto it = flattenedItem.begin(); it != flattenedItem.end(); ++it) {
                flattenedData[it.key()] = it.value();
            }
        }

        // Perform aggregation using MurmurHash
        std::unordered_map<std::string, int, MurmurHash> aggregationMap;
        jsonProcessor.processFlattenedJSON(flattenedData, aggregationMap);

        // Output the aggregation results
        std::cout << "Aggregated Results:" << std::endl;
        for (const auto& pair : aggregationMap) {
            std::cout << pair.first << ": " << pair.second << std::endl;
        }
//
//    } catch (const std::exception& ex) {
//        std::cerr << "Error: " << ex.what() << std::endl;
//        return 1;
//    }

    return 0;
}
