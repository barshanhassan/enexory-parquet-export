#include <iostream>
#include <unordered_map>
#include <vector>
#include <string>
#include <cstdint>
#include <sstream>
#include <regex>
#include <algorithm>

struct Change {
    char type;  // 'I', 'U', or 'D'
    std::string dt;
    double val;
    uint64_t ts;
};

std::string trim(const std::string& str) {
    size_t first = str.find_first_not_of(" \t");
    if (first == std::string::npos) return "";
    size_t last = str.find_last_not_of(" \t");
    return str.substr(first, (last - first + 1));
}

// Helper to process a single statement block and update consolidated map
void process_block(char type, const std::vector<std::string>& block, std::unordered_map<uint64_t, Change>& consolidated) {
    std::unordered_map<std::string, std::string> where_vals, set_vals;
    bool in_where = false;
    bool in_set = false;

    for (const auto& l : block) {
        std::string tl = trim(l);
        if (tl == "WHERE") {
            in_where = true;
            in_set = false;
            continue;
        }
        if (tl == "SET") {
            in_where = false;
            in_set = true;
            continue;
        }

        // Parse @n=value lines
        std::regex assignment(R"(^\s*@(\d+)=(.+)$)");
        std::smatch match;
        if (std::regex_match(tl, match, assignment)) {
            std::string col = "@" + match[1].str();
            std::string val = trim(match[2].str());

            // Handle quoted datetime strings (remove outer single quotes)
            if (!val.empty() && val.front() == '\'' && val.back() == '\'') {
                val = val.substr(1, val.size() - 2);
            }

            if (in_where) {
                where_vals[col] = val;
            } else if (in_set) {
                set_vals[col] = val;
            } else {
                // For INSERT (no WHERE/SET) or DELETE (no SET), treat as set/where
                if (type == 'I') {
                    set_vals[col] = val;
                } else if (type == 'D') {
                    where_vals[col] = val;
                }
            }
        }
    }

    // Select data source: SET for I/U, WHERE for D
    const auto& data_vals = (type == 'D' ? where_vals : set_vals);
    auto pk_it = data_vals.find("@1");
    if (pk_it == data_vals.end()) return;  // Missing PK, skip invalid block

    uint64_t pk = std::stoull(pk_it->second);
    std::string dt = data_vals.count("@3") ? data_vals.at("@3") : "";
    double val = data_vals.count("@4") ? std::stod(data_vals.at("@4")) : 0.0;
    uint64_t ts = data_vals.count("@6") ? std::stoull(data_vals.at("@6")) : 0ULL;

    auto it = consolidated.find(pk);
    if (type == 'I') {
        consolidated[pk] = {'I', dt, val, ts};
    } else if (type == 'U') {
        char cur_type = (it != consolidated.end()) ? it->second.type : 'U';
        if (cur_type != 'I') cur_type = 'U';
        consolidated[pk] = {cur_type, dt, val, ts};
    } else if (type == 'D') {
        if (it != consolidated.end() && it->second.type == 'I') {
            consolidated.erase(pk);
        } else {
            consolidated[pk] = {'D', dt, val, ts};
        }
    }
}

int main() {
    std::unordered_map<uint64_t, Change> consolidated;
    std::vector<std::string> current_block;
    char current_type = 0;  // 0: none, 'I': INSERT, 'U': UPDATE, 'D': DELETE

    std::string line;
    while (std::getline(std::cin, line)) {
        std::string tline = trim(line);
        if (tline.empty()) continue;

        // Detect statement type and start new block
        if (tline.find("INSERT INTO `enexory`.`api_data_timeseries`") == 0) {
            // Process previous block if any
            if (!current_block.empty() && current_type != 0) {
                process_block(current_type, current_block, consolidated);
                current_block.clear();
            }
            current_type = 'I';
        } else if (tline.find("UPDATE `enexory`.`api_data_timeseries`") == 0) {
            if (!current_block.empty() && current_type != 0) {
                process_block(current_type, current_block, consolidated);
                current_block.clear();
            }
            current_type = 'U';
        } else if (tline.find("DELETE FROM `enexory`.`api_data_timeseries`") == 0) {
            if (!current_block.empty() && current_type != 0) {
                process_block(current_type, current_block, consolidated);
                current_block.clear();
            }
            current_type = 'D';
        }

        // Add line to current block (skip if not part of statement body)
        if (current_type != 0) {
            current_block.push_back(line);  // Store original for accurate parsing
        }
    }

    // Process the last block
    if (!current_block.empty() && current_type != 0) {
        process_block(current_type, current_block, consolidated);
    }

    // Output consolidated changes
    for (const auto& p : consolidated) {
        uint64_t pk = p.first;
        const Change& change = p.second;
        std::cout << pk << ", '" << change.dt << "', " << change.val << ", " << change.ts << ", " << change.type << std::endl;
    }

    return 0;
}