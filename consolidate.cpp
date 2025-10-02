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
    char type;        // 'I', 'U'
    char dt[20];      // 'YYYY-MM-DD HH:MM:SS' in UTC+2
    double val;       // Numeric for non-NULL, use nan for NULL
    uint64_t ts;      // Unix timestamp
    bool val_is_null; // Flag for NULL value
    uint64_t pk;      // Primary key
};

struct DeletedEntry {
    uint64_t pk;      // Primary key
    char dt[20];      // 'YYYY-MM-DD HH:MM:SS' in UTC+2
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

inline void process_block(char type, uint64_t pk, const std::string& dt, 
                         const std::string& val_raw, uint64_t ts,
                         std::unordered_map<std::string, std::vector<Change>>& changes_by_day,
                         std::unordered_map<std::string, std::vector<DeletedEntry>>& deleted_by_day) {
    if (pk == 0) return;
    if (type != 'D' && (dt.empty() || ts == 0)) return;

    std::string day = dt.substr(0, 10);

    if (type == 'D') {
        auto& changes = changes_by_day[day];
        auto it = std::find_if(changes.begin(), changes.end(), 
                               [pk](const Change& c) { return c.pk == pk; });
        if (it != changes.end()) {
            if (it->type == 'I') {
                changes.erase(it);
                return;
            } else {
                changes.erase(it);
            }
        }
        DeletedEntry entry;
        entry.pk = pk;
        strncpy(entry.dt, dt.c_str(), sizeof(entry.dt) - 1);
        entry.dt[sizeof(entry.dt) - 1] = '\0';
        deleted_by_day[day].push_back(entry);
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
            return;
        }
    }

    Change change;
    change.type = type;
    change.pk = pk;
    change.val = val;
    change.val_is_null = val_is_null;
    change.ts = ts;
    strncpy(change.dt, dt.c_str(), sizeof(change.dt) - 1);
    change.dt[sizeof(change.dt) - 1] = '\0';

    auto& changes = changes_by_day[day];
    auto it = std::find_if(changes.begin(), changes.end(), 
                           [pk](const Change& c) { return c.pk == pk; });
    if (it == changes.end()) {
        changes.push_back(change);
    } else {
        if (it->type == 'I' && type == 'U') {
            change.type = 'I';
        }
        *it = change;
    }
}

void update_parquet_file(const std::string& day, const std::vector<Change>& changes, 
                        const std::vector<DeletedEntry>& deleted, const std::string& base_folder) {
    std::string file_path = base_folder + "/" + day + ".parquet";

    if (changes.empty() && deleted.empty()) {
        return;
    }

    auto id_field = arrow::field("id", arrow::uint64());
    auto dt_field = arrow::field("date_time", arrow::utf8());
    auto value_field = arrow::field("value", arrow::float64(), true);
    auto ts_field = arrow::field("ts", arrow::utf8());
    auto schema = arrow::schema({id_field, dt_field, value_field, ts_field});

    std::shared_ptr<arrow::Table> table;
    arrow::MemoryPool* pool = arrow::default_memory_pool();
    std::shared_ptr<arrow::io::ReadableFile> infile;
    bool file_exists = std::filesystem::exists(file_path);

    auto open_result = arrow::io::ReadableFile::Open(file_path, pool);
    if (open_result.ok()) {
        infile = *open_result;
        std::unique_ptr<parquet::arrow::FileReader> reader;
        auto reader_result = parquet::arrow::OpenFile(infile, pool);
        if (!reader_result.ok()) {
            std::cerr << "Failed to open Parquet reader for " << file_path << ": " << reader_result.status().message() << "\n";
            return;
        }
        reader = std::move(*reader_result);
        auto read_status = reader->ReadTable(&table);
        if (!read_status.ok()) {
            std::cerr << "Failed to read table from " << file_path << ": " << read_status.message() << "\n";
            return;
        }
    } else {
        std::vector<std::shared_ptr<arrow::Array>> arrays = {
            arrow::MakeArrayOfNull(arrow::uint64(), 0).ValueOrDie(),
            arrow::MakeArrayOfNull(arrow::utf8(), 0).ValueOrDie(),
            arrow::MakeArrayOfNull(arrow::float64(), 0).ValueOrDie(),
            arrow::MakeArrayOfNull(arrow::utf8(), 0).ValueOrDie()
        };
        table = arrow::Table::Make(schema, arrays);
    }

    std::vector<uint64_t> ids;
    std::vector<std::string> dts;
    std::vector<std::optional<double>> values;
    std::vector<std::string> tss;
    ids.reserve(table->num_rows());
    dts.reserve(table->num_rows());
    values.reserve(table->num_rows());
    tss.reserve(table->num_rows());

    // Iterate over chunks, then rows within that chunk.
    int num_chunks = table->num_rows() > 0 ? table->column(0)->num_chunks() : 0;

    for (int c = 0; c < num_chunks; ++c) {
        // Get arrays for the current chunk 'c'
        auto id_array = std::static_pointer_cast<arrow::UInt64Array>(table->column(0)->chunk(c));
        auto dt_array = std::static_pointer_cast<arrow::StringArray>(table->column(1)->chunk(c));
        auto value_array = std::static_pointer_cast<arrow::DoubleArray>(table->column(2)->chunk(c));
        auto ts_array = std::static_pointer_cast<arrow::StringArray>(table->column(3)->chunk(c));

        // Iterate only up to the length of THIS chunk
        for (int64_t i = 0; i < id_array->length(); ++i) {
            ids.push_back(id_array->Value(i));
            dts.push_back(dt_array->GetString(i));
            values.push_back(value_array->IsNull(i) ? std::nullopt : std::optional<double>(value_array->Value(i)));
            tss.push_back(ts_array->GetString(i));
        }
    }

    std::unordered_set<uint64_t> pks_to_remove;
    pks_to_remove.reserve(deleted.size() + changes.size());
    for (const auto& del : deleted) {
        pks_to_remove.insert(del.pk);
    }
    for (const auto& change : changes) {
        pks_to_remove.insert(change.pk);
    }

    std::vector<uint64_t> new_ids;
    std::vector<std::string> new_dts;
    std::vector<std::optional<double>> new_values;
    std::vector<std::string> new_tss;
    new_ids.reserve(ids.size() + changes.size());
    new_dts.reserve(ids.size() + changes.size());
    new_values.reserve(ids.size() + changes.size());
    new_tss.reserve(ids.size() + changes.size());

    for (size_t i = 0; i < ids.size(); ++i) {
        if (pks_to_remove.find(ids[i]) == pks_to_remove.end()) {
            new_ids.push_back(ids[i]);
            new_dts.push_back(dts[i]);
            new_values.push_back(values[i]);
            new_tss.push_back(tss[i]);
        }
    }

    for (const auto& change : changes) {
        new_ids.push_back(change.pk);
        new_dts.push_back(change.dt);
        new_values.push_back(change.val_is_null ? std::nullopt : std::optional<double>(change.val));
        new_tss.push_back(ts_to_utc2(change.ts));
    }

    // If the resulting table is empty and the file exists, delete it
    if (new_ids.empty() && file_exists) {
        try {
            std::filesystem::remove(file_path);
            std::cout << "Deleted " << file_path << ": No rows remain ("
                      << changes.size() << " changes, " << deleted.size() << " deletions)\n";
        } catch (const std::filesystem::filesystem_error& e) {
            std::cerr << "Failed to delete " << file_path << ": " << e.what() << "\n";
        }
        return;
    }
    // Skip writing if the resulting table is empty and no file exists
    if (new_ids.empty()) {
        std::cout << "Skipped writing " << file_path << ": No rows to write ("
                  << changes.size() << " changes, " << deleted.size() << " deletions)\n";
        return;
    }

    arrow::UInt64Builder id_builder;
    arrow::StringBuilder dt_builder, ts_builder;
    arrow::DoubleBuilder value_builder;
    for (size_t i = 0; i < new_ids.size(); ++i) {
        auto id_status = id_builder.Append(new_ids[i]);
        auto dt_status = dt_builder.Append(new_dts[i]);
        auto value_status = new_values[i] ? value_builder.Append(*new_values[i]) : value_builder.AppendNull();
        auto ts_status = ts_builder.Append(new_tss[i]);
        if (!id_status.ok() || !dt_status.ok() || !value_status.ok() || !ts_status.ok()) {
            std::cerr << "Failed to append to builders: " << id_status.message() << ", " << dt_status.message() << ", "
                      << value_status.message() << ", " << ts_status.message() << "\n";
            return;
        }
    }

    std::shared_ptr<arrow::Array> new_id_array, new_dt_array, new_value_array, new_ts_array;
    auto id_status = id_builder.Finish(&new_id_array);
    auto dt_status = dt_builder.Finish(&new_dt_array);
    auto value_status = value_builder.Finish(&new_value_array);
    auto ts_status = ts_builder.Finish(&new_ts_array);
    if (!id_status.ok() || !dt_status.ok() || !value_status.ok() || !ts_status.ok()) {
        std::cerr << "Failed to build arrays: " << id_status.message() << ", " << dt_status.message() << ", "
                  << value_status.message() << ", " << ts_status.message() << "\n";
        return;
    }

    auto new_table = arrow::Table::Make(schema, {new_id_array, new_dt_array, new_value_array, new_ts_array});

    std::shared_ptr<arrow::io::FileOutputStream> outfile;
    auto open_outfile = arrow::io::FileOutputStream::Open(file_path);
    if (!open_outfile.ok()) {
        if (file_exists) {
            std::cerr << "Failed to open " << file_path << ": " << open_outfile.status().message() << "\n";
        }
        return;
    }
    outfile = *open_outfile;

    parquet::WriterProperties::Builder builder;
    builder.compression(parquet::Compression::SNAPPY);
    auto write_status = parquet::arrow::WriteTable(*new_table, pool, outfile, 1024 * 1024, builder.build());
    if (!write_status.ok()) {
        std::cerr << "Failed to write table to " << file_path << ": " << write_status.message() << "\n";
        return;
    }

    std::cout << "Updated " << file_path << " with " << changes.size() << " changes and " 
              << deleted.size() << " deletions. New row count: " << new_table->num_rows() << "\n";
}

int main() {
    // Start timer
    auto start_time = std::chrono::high_resolution_clock::now();

    std::ios::sync_with_stdio(false);
    std::cin.tie(nullptr);

    std::unordered_map<std::string, std::vector<Change>> changes_by_day;
    std::unordered_map<std::string, std::vector<DeletedEntry>> deleted_by_day;
    changes_by_day.reserve(100);
    deleted_by_day.reserve(100);

    char current_type = 0;
    bool in_where = false, in_set = false;
    uint64_t pk = 0, ts = 0;
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
    for (auto it = changes_by_day.begin(); it != changes_by_day.end(); ) {
        const std::string& day = it->first;
        update_parquet_file(day, it->second, deleted_by_day[day], base_folder);
        it = changes_by_day.erase(it); // Clear memory for changes
        deleted_by_day.erase(day); // Clear memory for deletions
    }

    // End timer and print elapsed time
    auto end_time = std::chrono::high_resolution_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end_time - start_time);
    std::cout << "Total execution time: " << duration.count() / 1000.0 << " seconds\n";

    return 0;
}