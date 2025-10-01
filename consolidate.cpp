#include <iostream>
#include <unordered_map>
#include <vector>
#include <string>
#include <cstdint>
#include <cstring>

struct Change {
    char type;        // 'I', 'U'
    char dt[20];      // Fixed-size for 'YYYY-MM-DD HH:MM:SS'
    double val;       // Numeric for non-NULL, use nan for NULL
    uint64_t ts;      // Numeric
    bool val_is_null; // Flag for NULL value
};

struct DeletedEntry {
    uint64_t pk;      // Primary key
    char dt[20];      // Fixed-size for 'YYYY-MM-DD HH:MM:SS'
};

std::string trim(const std::string& str) {
    size_t start = 0;
    while (start < str.size() && (str[start] == ' ' || str[start] == '\t')) ++start;
    size_t end = str.size();
    while (end > start && (str[end - 1] == ' ' || str[end - 1] == '\t')) --end;
    return str.substr(start, end - start);
}

inline void process_block(char type, uint64_t pk, const std::string& dt, 
                         const std::string& val_raw, uint64_t ts,
                         std::unordered_map<uint64_t, Change>& consolidated,
                         std::vector<DeletedEntry>& deleted) {
    if (pk == 0) return; // Skip invalid block
    if (type != 'D' && (dt.empty() || ts == 0)) return; // Skip invalid INSERT/UPDATE

    // Handle DELETE
    if (type == 'D') {
        auto it = consolidated.find(pk);
        if (it != consolidated.end()) {
            if (it->second.type == 'I') {
                consolidated.erase(pk); // No-op for INSERT
            } else { // UPDATE
                consolidated.erase(pk);
                DeletedEntry entry;
                entry.pk = pk;
                strncpy(entry.dt, dt.c_str(), sizeof(entry.dt) - 1);
                entry.dt[sizeof(entry.dt) - 1] = '\0';
                deleted.push_back(entry);
            }
        } else {
            DeletedEntry entry;
            entry.pk = pk;
            strncpy(entry.dt, dt.c_str(), sizeof(entry.dt) - 1);
            entry.dt[sizeof(entry.dt) - 1] = '\0';
            deleted.push_back(entry);
        }
        return;
    }

    // Validate val for INSERT/UPDATE
    double val = 0.0;
    bool val_is_null = false;
    if (val_raw == "NULL") {
        val_is_null = true;
    } else {
        try {
            val = std::stod(val_raw);
        } catch (...) {
            return; // Invalid double, skip
        }
    }

    // Build Change for INSERT/UPDATE
    Change change;
    change.type = type;
    change.val = val;
    change.val_is_null = val_is_null;
    change.ts = ts;
    strncpy(change.dt, dt.c_str(), sizeof(change.dt) - 1);
    change.dt[sizeof(change.dt) - 1] = '\0'; // Ensure null-terminated

    // Consolidation logic for INSERT/UPDATE
    auto it = consolidated.find(pk);
    if (type == 'I') {
        consolidated[pk] = change;
    } else if (type == 'U') {
        char cur_type = (it != consolidated.end()) ? it->second.type : 'U';
        if (cur_type != 'I') cur_type = 'U';
        change.type = cur_type;
        consolidated[pk] = change;
    }
}

int main() {
    std::ios::sync_with_stdio(false); // Speed up I/O
    std::cin.tie(nullptr);            // Untie cin/cout for faster input

    std::unordered_map<uint64_t, Change> consolidated;
    std::vector<DeletedEntry> deleted;
    consolidated.reserve(1000000); // Pre-allocate for ~1M entries (~53MB)
    deleted.reserve(1000000);      // Pre-allocate for ~1M deletes (~28MB)
    char current_type = 0;         // 0: none, 'I': INSERT, 'U': UPDATE, 'D': DELETE
    bool in_where = false, in_set = false;
    uint64_t pk = 0, ts = 0;
    std::string dt, val_raw;
    char output_buffer[512]; // Fixed-size for CSV output

    std::string line;
    line.reserve(256); // Pre-allocate to reduce reallocations

    while (std::getline(std::cin, line)) {
        std::string tline = trim(line);
        if (tline.empty()) continue;

        // Detect new statement
        if (tline == "INSERT INTO `enexory`.`api_data_timeseries`") {
            if (current_type != 0 && pk != 0) {
                process_block(current_type, pk, dt, val_raw, ts, consolidated, deleted);
                pk = 0; ts = 0; dt.clear(); val_raw.clear();
            }
            current_type = 'I';
            in_where = false;
            in_set = false;
            continue;
        } else if (tline == "UPDATE `enexory`.`api_data_timeseries`") {
            if (current_type != 0 && pk != 0) {
                process_block(current_type, pk, dt, val_raw, ts, consolidated, deleted);
                pk = 0; ts = 0; dt.clear(); val_raw.clear();
            }
            current_type = 'U';
            in_where = false;
            in_set = false;
            continue;
        } else if (tline == "DELETE FROM `enexory`.`api_data_timeseries`") {
            if (current_type != 0 && pk != 0) {
                process_block(current_type, pk, dt, val_raw, ts, consolidated, deleted);
                pk = 0; ts = 0; dt.clear(); val_raw.clear();
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
            if (eq_pos == std::string::npos) continue;
            std::string col = tline.substr(0, eq_pos);
            std::string val = trim(tline.substr(eq_pos + 1));

            if (col == "@1") {
                pk = 0;
                for (char c : val) {
                    if (c < '0' || c > '9') { pk = 0; break; } // Non-numeric, invalidate
                    pk = pk * 10 + (c - '0');
                }
            } else if (col == "@3") { // Parse dt for all types
                dt = (val.size() > 2 && val.front() == '\'' && val.back() == '\'') ? 
                     val.substr(1, val.size() - 2) : val;
            } else if (current_type != 'D') { // Skip for DELETE
                if (col == "@4") {
                    val_raw = (val == "NULL") ? "NULL" : val;
                } else if (col == "@6") {
                    ts = 0;
                    for (char c : val) {
                        if (c < '0' || c > '9') { ts = 0; break; } // Non-numeric, invalidate
                        ts = ts * 10 + (c - '0');
                    }
                }
            }
        }
    }

    // Process final block
    if (current_type != 0 && pk != 0) {
        process_block(current_type, pk, dt, val_raw, ts, consolidated, deleted);
    }

    // Output consolidated changes as CSV (type first)
    for (const auto& p : consolidated) {
        uint64_t pk = p.first;
        const Change& change = p.second;
        int len = snprintf(output_buffer, sizeof(output_buffer), 
                          "%c,%lu,'%s',%s,%lu\n",
                          change.type, pk, change.dt, 
                          change.val_is_null ? "NULL" : std::to_string(change.val).c_str(), 
                          change.ts);
        std::cout.write(output_buffer, len);
    }
    // Output deleted rows (type 'D', pk, dt)
    for (const auto& entry : deleted) {
        int len = snprintf(output_buffer, sizeof(output_buffer), 
                          "D,%lu,'%s'\n", entry.pk, entry.dt);
        std::cout.write(output_buffer, len);
    }

    return 0;
}