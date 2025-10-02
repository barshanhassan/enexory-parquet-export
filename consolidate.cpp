#include <iostream>
#include <unordered_map>
#include <unordered_set>
#include <vector>
#include <string>
#include <cstdint>
#include <cstring>
#include <arrow/io/api.h>
#include <arrow/table.h>
#include <arrow/array.h>
#include <arrow/builder.h>
#include <parquet/arrow/reader.h>
#include <parquet/arrow/writer.h>
#include <date/date.h>
#include <chrono>
#include <thread>
#include <filesystem>
#include <optional>

struct Change {
    char dt[20];      // 'YYYY-MM-DD HH:MM:SS' in UTC+2
    double val;       // Numeric for non-NULL, use nan for NULL
    uint64_t ts;      // Unix timestamp
    bool val_is_null; // Flag for NULL value
    int64_t pk;      // Primary key
};

std::string trim(const std::string& str) {
    size_t start = 0;
    while (start < str.size() && (str[start] == ' ' || str[start] == '\t')) ++start;
    size_t end = str.size();
    while (end > start && (str[end - 1] == ' ' || str[end - 1] == '\t')) --end;
    return str.substr(start, end - start);
}

std::string ts_to_utc2(uint64_t ts) {
    auto tp = std::chrono::system_clock::from_time_t(ts);
    auto offset_tp = tp + std::chrono::hours(2);
    auto formatted = date::format("%F %T", offset_tp);
    if (formatted.size() > 19) {
        formatted = formatted.substr(0, 19);
    }
    return formatted;
}

inline void process_block(char type, int64_t pk, const std::string& dt, 
                         const std::string& val_raw, uint64_t ts,
                         std::unordered_map<std::string, std::unordered_map<int64_t, Change>>& changes_by_day,
                         std::unordered_map<std::string, std::unordered_set<int64_t>>& deleted_by_day) {
    if (pk == 0) throw std::runtime_error("Invalid event: Primary Key (pk) is 0.");
    if (dt.empty()) throw std::runtime_error("Invalid event: Date/Time (dt) is empty for pk " + std::to_string(pk));
    
    if (type != 'D' && ts == 0) throw std::runtime_error("Invalid event: Timestamp (ts) is 0 for INSERT/UPDATE on pk " + std::to_string(pk));

    std::string day = dt.substr(0, 10);

    if (type == 'D') {
        changes_by_day[day].erase(pk);
        deleted_by_day[day].insert(pk);
        return;
    }

    double val = 0.0;
    bool val_is_null = false;
    if (val_raw == "NULL") {
        val_is_null = true;
    } else {
        try {
            val = std::stod(val_raw);
        } catch (...) {
            throw std::runtime_error("Failed to parse value '" + val_raw + "' for pk " + std::to_string(pk) + ". Details: " + e.what());
        }
    }

    Change change;
    change.pk = pk;
    change.val = val;
    change.val_is_null = val_is_null;
    change.ts = ts;
    strncpy(change.dt, dt.c_str(), sizeof(change.dt) - 1);
    change.dt[sizeof(change.dt) - 1] = '\0';

    changes_by_day[day][pk] = change;
}

void update_parquet_file(const std::string& day, const std::unordered_map<int64_t, Change>& upserts, 
                        const std::unordered_set<int64_t>& deletes, const std::string& base_folder) {
    std::string file_path = base_folder + "/" + day + ".parquet";

    if (upserts.empty() && deletes.empty()) return;

    auto id_field = arrow::field("id", arrow::int64());
    auto dt_field = arrow::field("date_time", arrow::utf8());
    auto value_field = arrow::field("value", arrow::float64(), true);
    auto ts_field = arrow::field("ts", arrow::utf8());
    auto schema = arrow::schema({id_field, dt_field, value_field, ts_field});

    struct RowData {
        std::string dt;
        std::optional<double> value;
        std::string ts;
    };
    std::unordered_map<int64_t, RowData> in_memory_table;

    arrow::MemoryPool* pool = arrow::default_memory_pool();
    bool file_exists = std::filesystem::exists(file_path);

    if (file_exists) {
        std::shared_ptr<arrow::io::ReadableFile> infile;
        auto open_result = arrow::io::ReadableFile::Open(file_path, pool);
        if (!open_result.ok()) {
            std::string error_msg = "Failed to open existing file " + file_path + ": " + open_result.status().message();
            std::cerr << error_msg << "\n";
            throw std::runtime_error(error_msg);
        }
        infile = *open_result;

        std::unique_ptr<parquet::arrow::FileReader> reader;
        auto reader_result = parquet::arrow::OpenFile(infile, pool);
        if (reader_result.ok()) {
            reader = std::move(*reader_result);
            std::shared_ptr<arrow::Table> table;
            if (reader->ReadTable(&table).ok() && table->num_rows() > 0) {
                in_memory_table.reserve(table->num_rows());
                for (int c = 0; c < table->column(0)->num_chunks(); ++c) {
                    auto id_array = std::static_pointer_cast<arrow::Int64Array>(table->column(0)->chunk(c));
                    auto dt_array = std::static_pointer_cast<arrow::StringArray>(table->column(1)->chunk(c));
                    auto value_array = std::static_pointer_cast<arrow::DoubleArray>(table->column(2)->chunk(c));
                    auto ts_array = std::static_pointer_cast<arrow::StringArray>(table->column(3)->chunk(c));

                    for (int64_t i = 0; i < id_array->length(); ++i) {
                        in_memory_table[id_array->Value(i)] = {
                            dt_array->GetString(i),
                            value_array->IsNull(i) ? std::nullopt : std::optional<double>(value_array->Value(i)),
                            ts_array->GetString(i)
                        };
                    }
                }
            }
        }
    }

    for (const auto& pair : upserts) {
        const int64_t& pk = pair.first;
        const Change& change = pair.second;
        in_memory_table[pk] = {
            std::string(change.dt),
            change.val_is_null ? std::nullopt : std::optional<double>(change.val),
            ts_to_utc2(change.ts)
        };
    }

    for (const auto& pk_to_delete : deletes) {
        in_memory_table.erase(pk_to_delete);
    }
    
    if (in_memory_table.empty()) {
        if (file_exists) {
            try {
                std::filesystem::remove(file_path);
                std::cout << "Deleted " << file_path << ": No rows remain.\n";
            } catch (const std::filesystem::filesystem_error& e) {
                std::cerr << "Failed to delete " << file_path << ": " << e.what() << "\n";
                throw;
            }
        }
        return;
    }

    arrow::Int64Builder id_builder;
    arrow::StringBuilder dt_builder, ts_builder;
    arrow::DoubleBuilder value_builder;

    for (const auto& pair : in_memory_table) {
        id_builder.Append(pair.first);
        dt_builder.Append(pair.second.dt);
        ts_builder.Append(pair.second.ts);
        if (pair.second.value.has_value()) {
            value_builder.Append(*pair.second.value);
        } else {
            value_builder.AppendNull();
        }
    }

    std::shared_ptr<arrow::Array> new_id_array, new_dt_array, new_value_array, new_ts_array;
    id_builder.Finish(&new_id_array);
    dt_builder.Finish(&new_dt_array);
    value_builder.Finish(&new_value_array);
    ts_builder.Finish(&new_ts_array);

    auto new_table = arrow::Table::Make(schema, {new_id_array, new_dt_array, new_value_array, new_ts_array});

    std::shared_ptr<arrow::io::FileOutputStream> outfile;
    auto open_outfile = arrow::io::FileOutputStream::Open(file_path);
    if (!open_outfile.ok()) {
        std::string error_msg = "Failed to open " + file_path + " for writing: " + open_outfile.status().message();
        std::cerr << error_msg << "\n";
        throw std::runtime_error(error_msg);
    }
    outfile = *open_outfile;

    parquet::WriterProperties::Builder writer_props_builder;
    writer_props_builder.compression(parquet::Compression::SNAPPY);
    auto write_status = parquet::arrow::WriteTable(*new_table, pool, outfile, 1024 * 1024, writer_props_builder.build());
    if (!write_status.ok()) {
        std::string error_msg = "Failed to write table to " + file_path + ": " + write_status.message();
        std::cerr << error_msg << "\n";
        throw std::runtime_error(error_msg);
    }

    std::cout << "Updated " << file_path << ". New row count: " << new_table->num_rows() << "\n";
}

int main() {
    try {
        // Start timer
        auto start_time = std::chrono::high_resolution_clock::now();

        std::ios::sync_with_stdio(false);
        std::cin.tie(nullptr);

        std::unordered_map<std::string, std::unordered_map<int64_t, Change>> changes_by_day;
        std::unordered_map<std::string, std::unordered_set<int64_t>> deleted_by_day;
        changes_by_day.reserve(100);
        deleted_by_day.reserve(100);

        char current_type = 0;
        bool in_where = false, in_set = false;
        int64_t pk = 0;
        uint64_t ts = 0;
        std::string dt, val_raw;
        std::string line;
        line.reserve(256);

        while (std::getline(std::cin, line)) {
            std::string tline = trim(line);
            if (tline.empty()) continue;

            if (tline == "INSERT INTO `enexory`.`api_data_timeseries`") {
                if (current_type != 0 && pk != 0) {
                    process_block(current_type, pk, dt, val_raw, ts, changes_by_day, deleted_by_day);
                    pk = 0; ts = 0; dt.clear(); val_raw.clear();
                }
                current_type = 'I';
                in_where = false;
                in_set = false;
                continue;
            } else if (tline == "UPDATE `enexory`.`api_data_timeseries`") {
                if (current_type != 0 && pk != 0) {
                    process_block(current_type, pk, dt, val_raw, ts, changes_by_day, deleted_by_day);
                    pk = 0; ts = 0; dt.clear(); val_raw.clear();
                }
                current_type = 'U';
                in_where = false;
                in_set = false;
                continue;
            } else if (tline == "DELETE FROM `enexory`.`api_data_timeseries`") {
                if (current_type != 0 && pk != 0) {
                    process_block(current_type, pk, dt, val_raw, ts, changes_by_day, deleted_by_day);
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

            if (current_type != 0 && tline.size() > 3 && tline[0] == '@') {
                size_t eq_pos = tline.find('=');
                if (eq_pos == std::string::npos) continue;
                std::string col = tline.substr(0, eq_pos);
                std::string val = trim(tline.substr(eq_pos + 1));

                if (col == "@1") {
                    pk = 0;
                    for (char c : val) {
                        if (c < '0' || c > '9') { pk = 0; break; }
                        pk = pk * 10 + (c - '0');
                    }
                } else if (col == "@3") {
                    dt = (val.size() > 2 && val.front() == '\'' && val.back() == '\'') ? 
                        val.substr(1, val.size() - 2) : val;
                } else if (current_type != 'D') {
                    if (col == "@4") {
                        val_raw = (val == "NULL") ? "NULL" : val;
                    } else if (col == "@6") {
                        ts = 0;
                        for (char c : val) {
                            if (c < '0' || c > '9') { ts = 0; break; }
                            ts = ts * 10 + (c - '0');
                        }
                    }
                }
            }
        }

        if (current_type != 0 && pk != 0) {
            process_block(current_type, pk, dt, val_raw, ts, changes_by_day, deleted_by_day);
        }

        const std::string base_folder = "/root/data";
        std::filesystem::create_directories(base_folder);

        std::unordered_set<std::string> days_to_process_set;
        for (const auto& pair : changes_by_day) {
            days_to_process_set.insert(pair.first);
        }
        for (const auto& pair : deleted_by_day) {
            days_to_process_set.insert(pair.first);
        }
        std::vector<std::string> days_to_process(days_to_process_set.begin(), days_to_process_set.end());


        for (const std::string& day : days_to_process) {
            const static std::unordered_map<int64_t, Change> empty_changes;
            const static std::unordered_set<int64_t> empty_deletes;

            auto changes_it = changes_by_day.find(day);
            const auto& upserts = (changes_it != changes_by_day.end()) 
                                    ? changes_it->second 
                                    : empty_changes;

            auto deleted_it = deleted_by_day.find(day);
            const auto& deletes = (deleted_it != deleted_by_day.end()) 
                                    ? deleted_it->second 
                                    : empty_deletes;

            update_parquet_file(day, upserts, deletes, base_folder);
        }

        // End timer and print elapsed time
        auto end_time = std::chrono::high_resolution_clock::now();
        auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end_time - start_time);
        std::cout << "Total execution time: " << duration.count() / 1000.0 << " seconds\n";

    } catch (const std::exception& e) {
        std::cerr << "An unrecoverable error occurred: " << e.what() << std::endl;
        return 1; // Exit with a non-zero status code to indicate failure
    }

    return 0;
}