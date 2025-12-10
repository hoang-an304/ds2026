#include <iostream>
#include <fstream>
#include <sstream>
#include <string>
#include <map>
#include <algorithm>
#include <cctype>

using namespace std; 

void map_function(const string& input_filename, map<string, int>& intermediate_data) {
    
    ifstream input_file(input_filename);
    if (!input_file.is_open()) {
        cerr << "Error: Could not open input file " << input_filename << endl;
        return;
    }

    string line;
    while (getline(input_file, line)) {
        stringstream ss(line);
        string word;
        while (ss >> word) {
            string cleaned_word;
            for (char c : word) {
                if (isalpha(c)) { 
                    cleaned_word += tolower(c);
                }
            }

            if (!cleaned_word.empty()) {
                intermediate_data[cleaned_word]++;
            }
        }
    }
    cout << "Map and in-memory Shuffle/Sort phases complete." << endl;
}

void reduce_function(const map<string, int>& intermediate_data, const string& output_filename) {                
    ofstream output_file(output_filename);
    if (!output_file.is_open()) {
        cerr << "Error: Could not open output file " << output_filename << endl;
        return;
    }
    for (const auto& pair : intermediate_data) {
        output_file << pair.first << "\t" << pair.second << "\n";
    }
    cout << "Reduce phase complete. Final counts written to " << output_filename << endl;
}

int main(int argc, char *argv[]) {
    if (argc != 2) {
        cerr << "Usage: " << argv[0] << " <input_file_path>" << endl;
        return 1;
    }
    map<string, int> word_counts;
    
    string input_file = argv[1];
    string output_file = "output.txt";

    map_function(input_file, word_counts);
    reduce_function(word_counts, output_file);
    cout << "\nWord count finished." << endl;

    return 0;
}