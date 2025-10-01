#include <iostream>
#include <unordered_map>
#include <string>
#include <cstdint>
#include <cstring>

struct Change {
    char type;  // 'I', 'U', 'D'
    std::string dt;  // Keep as string for datetime (quoted in output)
    std::string val; // String to handle NULL
    uint64_t ts;     // Numeric for efficiency
};

std::string_view trim(std::string_view sv) {
    size_t start = 0;
    while (start < sv.size() && (sv[start] == ' ' || sv[start] == '\t')) ++start;
    size_t end = sv.size();
    while (end > start && (sv[end - 1] == ' ' || sv[end - 1] == '\t')) --end;
    return sv.substr(start, end - start);
}

int main() {
    std::ios::sync_with_stdio(false); // Speed up I/O
    std::cin.tie(nullptr);            // Untie cin/cout for faster input

    std::unordered_map<uint64_t, Change> consolidated;
    char current_type = 0;  // 0: none, 'I': INSERT, 'U': UPDATE, 'D': DELETE
    bool in_where = false, in_set = false;
    uint64_t pk = 0;
    std::string dt, val_raw, ts_raw;
    char output_buffer[512]; // Fixed-size for CSV output (adjust if needed)

    std::string line;
    line.reserve(256); // Pre-allocate to reduce reallocations

    while (std::getline(std::cin, line)) {
        std::string_view tline = trim(line);
        if (tline.empty()) continue;

        // Detect new statement
        if (tline == "INSERT INTO `enexory`.`api_data_timeseries`") {
            // Process previous block if complete
            if (current_type != 0 && pk != 0) {
                process_block(current_type, pk, dt, val_raw, ts_raw, consolidated);
                pk = 0; dt.clear(); val_raw.clear(); ts_raw.clear();
            }
            current_type = 'I';
            in_where = false;
            in_set = false;
            continue;
        } else if (tline == "UPDATE `enexory`.`api_data_timeseries`") {
            if (current_type != 0 && pk != 0) {
                process_block(current_type, pk, dt, val_raw, ts_raw, consolidated);
                pk = 0; dt.clear(); val_raw.clear(); ts_raw.clear();
            }
            current_type = 'U';
            in_where = false;
            in_set = false;
            continue;
        } else if (tline == "DELETE FROM `enexory`.`api_data_timeseries`") {
            if (current_type != 0 && pk != 0) {
                process_block(current_type, pk, dt, val_raw, ts_raw, consolidated);
                pk = 0; dt.clear(); val_raw.clear(); ts_raw.clear();
            }
            current_type = 'D';
            in_where = false;
            in_set = false;
            continue;
        } else if (tline == "WHERE") {
            in_where = true;
            in_set = false;
            continue;
        } else if (tline == "SET") {
            in_where = false;
            in_set = true;
            continue;
        }

        // Parse @n=value lines
        if (current_type != 0 && tline.size() > 3 && tline[0] == '@') {
            size_t eq_pos = tline.find('=');
            if (eq_pos == std::string_view::npos) continue;
            std::string_view col = tline.substr(0, eq_pos);
            std::string_view val = trim(tline.substr(eq_pos + 1));

            if (col == "@1") {
                pk = 0;
                for (char c : val) {
                    if (c < '0' || c > '9') break; // Non-numeric, skip
                    pk = pk * 10 + (c - '0');
                }
            } else if (col == "@3") {
                dt = (val.size() > 2 && val.front() == '\'' && val.back() == '\'') ? 
                     std::string(val.substr(1, val.size() - 2)) : std::string(val);
            } else if (col == "@4") {
                val_raw = (val == "NULL") ? "NULL" : std::string(val);
            } else if (col == "@6") {
                ts_raw = std::string(val);
            }
        }
    }

    // Process final block
    if (current_type != 0 && pk != 0) {
        process_block(current_type, pk, dt, val_raw, ts_raw, consolidated);
    }

    // Output consolidated changes as CSV
    for (const auto& p : consolidated) {
        uint64_t pk = p.first;
        const Change& change = p.second;
        int len = snprintf(output_buffer, sizeof(output_buffer), 
                          "%llu,'%s',%s,%llu,%c\n",
                          pk, change.dt.c_str(), change.val.c_str(), change.ts, change.type);
        std::cout.write(output_buffer, len);
    }

    return 0;
}

inline void process_block(char type, uint64_t pk, const std::string& dt, 
                         const std::string& val_raw, const std::string& ts_raw,
                         std::unordered_map<uint64_t, Change>& consolidated) {
    if (pk == 0 || ts_raw.empty()) return; // Skip invalid block

    // Validate and convert ts
    uint64_t ts = 0;
    for (char c : ts_raw) {
        if (c < '0' || c > '9') return; // Non-numeric, skip
        ts = ts * 10 + (c - '0');
    }

    // Validate val (NULL or valid double)
    std::string val = "NULL";
    if (val_raw != "NULL") {
        bool valid = true;
        try {
            double dummy = std::stod(val_raw);
            val = val_raw;
        } catch (...) {
            valid = false;
        }
        if (!valid) return; // Invalid double, skip
    }

    // Consolidation logic
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