#include <random>
#include <fstream>
#include <iostream>

#include "history.h"
#include "util.h"

namespace baidu::minirec {

void History::set_capacity(size_t bytes) {
    _capacity_bytes = bytes;
}

void History::init(const ::std::string& file_name) {
    ::std::ifstream ifs(file_name);
    if (!ifs) {
        ::std::cerr << "open user embedding file " << file_name << " failed" << ::std::endl;
        return;
    }

    size_t num = 0;
    ::std::string line;
    ::std::vector<::std::string_view> fields;
    ::std::vector<::std::string_view> inner_fields;

    std::random_device rd;  // Will be used to obtain a seed for the random number engine
    std::mt19937 gen(rd()); // Standard mersenne_twister_engine seeded with rd()
    std::uniform_int_distribution<> distrib(1000000000, 2000000000);

    while (::std::getline(ifs, line)) {
        split(line, '\t', fields);
        if (fields.size() < 2) {
            ::std::cerr << "illegal user file" << ::std::endl;
            return;
        }

        std::string show_item_list;
        for (std::size_t i = 0; i < 100; ++i) {
            if (!show_item_list.empty()) {
                show_item_list.append(1, ',');
            }

            show_item_list.append(std::to_string(distrib(gen)));
        }

        _showed_items_per_user.emplace(fields[0], ::std::move(show_item_list));
        num++;
    }
    ::std::cerr << "init " << num << " users' history" << ::std::endl;
}

int History::read(const ::std::string& user_id, ::std::string& showed_items_line) {
    ::std::shared_lock lock(_mutex);
    showed_items_line = _showed_items_per_user[user_id];
    return 0;
}

int History::append(const ::std::string& user_id, ::std::string_view new_showed_items_line) {
    if (new_showed_items_line.empty()) {
        return 0;
    }

    ::std::unique_lock lock(_mutex);
    auto& base_showed_items_line = _showed_items_per_user[user_id];

    // 如果超长则先尝试截断
    if (base_showed_items_line.size() + new_showed_items_line.size() > _capacity_bytes) {
        size_t erase_size = new_showed_items_line.size() + base_showed_items_line.size() - _capacity_bytes;
        // 对齐到下一个分割点
        size_t next_comma_pos = base_showed_items_line.find_first_of(',', erase_size);
        if (next_comma_pos != ::std::string::npos) {
            base_showed_items_line.erase(0, next_comma_pos + 1);
        } else {
            base_showed_items_line.clear();
        }
    }

    // 尾部追加
    if (!base_showed_items_line.empty()) {
        base_showed_items_line.append(1, ',');
    }

    base_showed_items_line.append(new_showed_items_line);

    // 1000us 模拟把用户下发历史写到远程的网络IO耗时, 这里不要改动
    usleep(1000);

    return 0;
}

}
