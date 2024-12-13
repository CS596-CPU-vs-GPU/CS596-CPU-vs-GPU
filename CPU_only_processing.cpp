//
// Created by Ananya Reddy on 12/12/2024.
//
#include <iostream>
#include <unordered_map>
#include <fstream>
#include <nlohmann/json.hpp>

using json = nlohmann::json;

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

    // Function to process the flattened JSON and perform aggregation
    void processFlattenedJSON(const json& flattenedData, std::unordered_map<std::string, int>& aggregationMap) {
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
        std::cout << "Starting CPU-only JSON processing..." << std::endl;

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

        // Perform aggregation
        std::unordered_map<std::string, int> aggregationMap;
        jsonProcessor.processFlattenedJSON(flattenedData, aggregationMap);

        // Output the aggregation results
        std::cout << "Aggregated Results:" << std::endl;
        for (const auto& pair : aggregationMap) {
            std::cout << pair.first << ": " << pair.second << std::endl;
        }

//    } catch (const std::exception& ex) {
//        std::cerr << "Error: " << ex.what() << std::endl;
//        return 1;
//    }

    return 0;
}
