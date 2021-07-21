#include "recommender.h"
#include "util.h"

#include <charconv>
#include <fstream>
#include <iostream>
#include <thread>

using baidu::minirec::History;
using baidu::minirec::Index;
using baidu::minirec::Recommender;
using baidu::minirec::monotonic_time_us;
using baidu::minirec::split;
using baidu::minirec::string_to_uint64;

static void keep_update(Index& index, size_t update_qps) {
    int64_t interval_us = 1000000L / update_qps;

    ::std::ifstream ifs;
    ifs.open("data/updates");

    int64_t begin_us = monotonic_time_us();
    ::std::string line;
    while (::std::getline(ifs, line)) {
        if (0 != index.add_item(line)) {
            ::std::cerr << "add item line failed" << std::endl;
            continue;
        }
        int64_t end_us = monotonic_time_us();
        int64_t used_us = end_us - begin_us;
        if (used_us < interval_us) {
            ::usleep(interval_us - used_us);
            begin_us = end_us + interval_us;
        }
    }
    ::std::cerr << "updater exit" << std::endl;
}

static void keep_query(Recommender& recommender, const ::std::vector<::std::string>& used_ids) {
    ::std::string item_ids_line;
    for (const auto& used_id : used_ids) {
        if (0 != recommender.recommend(used_id, item_ids_line)) {
            ::std::cerr << "recommand failed" << std::endl;
            continue;
        }
    }
    ::std::cerr << "query agent exit" << std::endl;
}

static void start_query_threads(Recommender& recommender, ::std::vector<::std::thread>& threads) {

    ::std::ifstream ifs("data/querys");
    if (!ifs) {
        ::std::cerr << "open querys file failed" << std::endl;
        return;
    }

    ::std::vector<::std::string_view> fields;
    ::std::string line;
    while (::std::getline(ifs, line)) {
        split(line, '\t', fields);
        ::std::vector<::std::string> used_ids;
        for (auto field : fields) {
            used_ids.emplace_back(field);
        }
        threads.emplace_back(keep_query,
                ::std::ref(recommender), used_ids);
    }
}

int main(int argc, char** argv) {
    size_t update_qps = 10;

    // 说明：加载用户的向量表示
    Index index;
    if (0 != index.load_user_embeddings("data/users")) {
        ::std::cerr << "load user embeddings from file failed" << std::endl;
        return 1;
    }

    // 说明：加载item的向量表示
    if (0 != index.load_items("data/items")) {
        ::std::cerr << "load items from file failed" << std::endl;
        return 1;
    }

    // 说明：用户的历史推荐记录
    History history;
    history.init("data/users");
    history.set_capacity(40 << 10);

    Recommender recommender;
    // 说明：加载item->item的相似度矩阵
    if (0 != recommender.load_similarity_matrix("data/similarity")) {
        ::std::cerr << "load similarity matrix from file failed" << std::endl;
        return 1;
    }
    recommender.set_index(index);
    recommender.set_history(history);
    recommender.set_index_candidate_num(512);

    // 说明：模拟索引更新
    ::std::thread updater(keep_update, ::std::ref(index), update_qps);

    // 说明：模拟在线请求
    ::std::vector<::std::thread> query_agent_threads;
    start_query_threads(recommender, query_agent_threads);

    updater.join();
    for (auto& thread : query_agent_threads) {
        thread.join();
    }
    return 0;
}
