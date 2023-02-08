#include "randomgenerator.h"

#include <algorithm>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <ctime>

#include "assert.h"

namespace tpcc {

    NURandC NURandC::makeRandom(RandomGenerator *generator) {
        NURandC c;
        c.c_last_ = generator->number(0, 255);
        c.c_id_ = generator->number(0, 1023);
        c.ol_i_id_ = generator->number(0, 8191);
        return c;
    }

// Returns true if the C-Run value is valid. See TPC-C 2.1.6.1 (page 20).
    static bool validCRun(int cRun, int cLoad) {
        int cDelta = abs(cRun - cLoad);
        return 65 <= cDelta && cDelta <= 119 && cDelta != 96 && cDelta != 112;
    }

    NURandC NURandC::makeRandomForRun(RandomGenerator *generator, const NURandC &c_load) {
        NURandC c = makeRandom(generator);

        while (!validCRun(c.c_last_, c_load.c_last_)) {
            c.c_last_ = generator->number(0, 255);
        }
        ASSERT(validCRun(c.c_last_, c_load.c_last_));

        return c;
    }

    int RandomGenerator::numberExcluding(int lower, int upper, int excluding) {
        ASSERT(lower < upper);
        ASSERT(lower <= excluding && excluding <= upper);

        // Generate 1 less number than the range
        int num = number(lower, upper - 1);

        // Adjust the numbers to remove excluding
        if (num >= excluding) {
            num += 1;
        }
        ASSERT(lower <= num && num <= upper && num != excluding);
        return num;
    }

    static void
    generateString(RandomGenerator *generator, char *s, int lower_length, int upper_length,
                   char base_character, int num_characters) {
        int length = generator->number(lower_length, upper_length);
        for (int i = 0; i < length; ++i) {
            s[i] = static_cast<char>(base_character + generator->number(0, num_characters - 1));
        }
        s[length] = '\0';
    }

    void RandomGenerator::astring(char *s, int lower_length, int upper_length, int cardinality) {
        generateString(this, s, lower_length, upper_length, 'a', cardinality);
    }

    void RandomGenerator::nstring(char *s, int lower_length, int upper_length) {
        generateString(this, s, lower_length, upper_length, '0', 10);
    }

    void RandomGenerator::distInfo(char *s, int d_id, int w_id, int i_id) {
        snprintf(s, 24 + 1, "dist-info-str#%02d#%03d#%03d", d_id, w_id, i_id);
    }

    void RandomGenerator::lastName(char *c_last, int max_cid) {
        makeLastName(NURand(255, 0, std::min(999, max_cid - 1)), c_last);
    }

    float RandomGenerator::fixedPoint(int digits, float lower, float upper) {
        int multiplier = 1;
        for (int i = 0; i < digits; ++i) {
            multiplier *= 10;
        }

        int int_lower = static_cast<int>(lower * static_cast<double>(multiplier) + 0.5);
        int int_upper = static_cast<int>(upper * static_cast<double>(multiplier) + 0.5);
        return (float) number(int_lower, int_upper) / (float) multiplier;
    }

    int RandomGenerator::NURand(int A, int x, int y) {
        int C = 0;
        switch (A) {
            case 255:
                C = c_values_.c_last_;
                break;
            case 1023:
                C = c_values_.c_id_;
                break;
            case 8191:
                C = c_values_.ol_i_id_;
                break;
            default:
                fprintf(stderr, "Error: NURand: A = %d not supported\n", A);
                exit(1);
        }
        return (((number(0, A) | number(x, y)) + C) % (y - x + 1)) + x;
    }

    int *RandomGenerator::makePermutation(int lower, int upper) {
        // initialize with consecutive values
        int *array = new int[upper - lower + 1];
        for (int i = 0; i <= upper - lower; ++i) {
            array[i] = lower + i;
        }

        for (int i = 0; i < upper - lower; ++i) {
            // choose a value to go into this position, including this position
            int index = number(i, upper - lower);
            int temp = array[i];
            array[i] = array[index];
            array[index] = temp;
        }

        return array;
    }

    bool RandomGenerator::loadDataDist() {
        std::ifstream in("data_dist/stock_ytd_1m.txt");
        if (in.fail()) {
            printf("Failed to open stock_ytd_1m.txt\n");
            return false;
        }

        std::string line;
        while (std::getline(in, line)) {
            if (line.back() == '\r') line.pop_back();
            stock_ytd_dist_.push_back(std::stoi(line));
        }
        in.close();

        in.open("data_dist/stock_order_cnt_1m.txt");
        if (in.fail()) {
            printf("Failed to open stock_order_cnt_1m.txt\n");
            return false;
        }

        while (std::getline(in, line)) {
            if (line.back() == '\r') line.pop_back();
            stock_order_cnt_dist_.push_back(std::stoi(line));
        }
        in.close();

        in.open("data_dist/stock_remote_cnt_1m.txt");
        if (in.fail()) {
            printf("Failed to open stock_remote_cnt_1m.txt\n");
            return false;
        }

        while (std::getline(in, line)) {
            if (line.back() == '\r') line.pop_back();
            stock_remote_cnt_dist_.push_back(std::stoi(line));
        }
        in.close();

        in.open("data_dist/customer_delivery_cnt_1m.txt");
        if (in.fail()) {
            printf("Failed to open customer_delivery_cnt_1m.txt\n");
            return false;
        }

        while (std::getline(in, line)) {
            if (line.back() == '\r') line.pop_back();
            cus_delivery_cnt_dist_.push_back(std::stoi(line));
        }
        in.close();

        in.open("data_dist/customer_balance_1m.txt");
        if (in.fail()) {
            printf("Failed to open customer_balance_1m.txt\n");
            return false;
        }

        while (std::getline(in, line)) {
            if (line.back() == '\r') line.pop_back();
            cus_balance_dist_.push_back(std::stof(line));
        }
        in.close();

        in.open("data_dist/customer_ytd_payment_1m.txt");
        if (in.fail()) {
            printf("Failed to open customer_ytd_payment_1m.txt\n");
            return false;
        }

        while (std::getline(in, line)) {
            if (line.back() == '\r') line.pop_back();
            cus_ytd_payment_dist_.push_back(std::stof(line));
        }
        in.close();

        in.open("data_dist/customer_payment_cnt_1m.txt");
        if (in.fail()) {
            printf("Failed to open customer_payment_cnt_1m.txt\n");
            return false;
        }

        while (std::getline(in, line)) {
            if (line.back() == '\r') line.pop_back();
            cus_payment_cnt_dist_.push_back(std::stoi(line));
        }
        in.close();
        return true;
    }

    void RandomGenerator::stockData(char *s, int upper_length) {
        std::vector<int> word_idx(4);
        int total_length = upper_length + 1;
        while (total_length > upper_length) {
            total_length = 3;
            for (int i = 0; i < 4; i++) {
                word_idx[i] = number(0, stock_data_corpus_.size() - 1);
                total_length += stock_data_corpus_[word_idx[i]].size();
            }
        }
        for (int i = 0; i < 4; i++) {
            std::string &word = stock_data_corpus_[word_idx[i]];
            strncpy(s, word.c_str(), word.size());

            s += word.size();
            if (i != 3) {
                *s = ' ';
                s++;
            }
        }
        *s = '\0';
    }

    void RandomGenerator::customerData(char *s, int upper_length, bool bad_credit) {
        int32_t word_num = 10;
        std::vector<int> word_idx(word_num);
        int total_length = upper_length + 1;
        while (total_length > upper_length) {
            total_length = word_num - 1;
            for (int i = 0; i < word_num; i++) {
                word_idx[i] = number(0, stock_data_corpus_.size() - 1);
                total_length += stock_data_corpus_[word_idx[i]].size();
            }
        }
        for (int i = 0; i < word_num; i++) {
            std::string &word = stock_data_corpus_[word_idx[i]];
            strncpy(s, word.c_str(), word.size());

            s += word.size();
            if (i != word_num - 1) {
                *s = ' ';
                s++;
            }
        }
        *s = '\0';

        // Bad credit: insert history into c_data
        if (bad_credit) {
            static const int HISTORY_SIZE = 30;
            char history[HISTORY_SIZE];
            int characters = snprintf(history, HISTORY_SIZE, " %04d-%02d-%03d-%02d-%03d-%04d",
                                      number(1, 3000), number(1, 10), number(1, 100), number(1, 10),
                                      number(1, 100), number(1, 10000));
            if (total_length + HISTORY_SIZE < upper_length) {
                strncpy(s, history, characters);
                s += characters;
                *s = '\0';
            }
        }
    }

    void RandomGenerator::phoneData(char *s, int length) {
        if (length == 16) {
            memcpy(s, "+01-", 4);
            std::string &word = phone_district_code[number(0, phone_district_code.size() - 1)];
            memcpy(s + 4, word.c_str(), 3);
            s[7] = '-';
            nstring(s + 8, 3, 3);
            s[11] = '-';
            nstring(s + 12, 4, 4);
        }
    }

    void RandomGenerator::departmentData(char *s, int upper_length) {
        if (upper_length > 13) {
            memcpy(s, "Department#", 11);
            nstring(s + 11, 1, 2);
        }
    }

    void RandomGenerator::customerString(char *s, int upper_length,
                                         const std::string &corpus_name) {
        std::vector<std::string> *corpus;
        if (corpus_name == "first_name") {
            corpus = &first_names_;
        } else if (corpus_name == "street") {
            corpus = &street_;
        } else if (corpus_name == "city") {
            corpus = &city_;
        } else if (corpus_name == "state") {
            corpus = &state_;
        } else if (corpus_name == "zip") {
            corpus = &zip_;
        } else if (corpus_name == "stock_data") {
            corpus = &stock_data_corpus_;
        } else {
            printf("Corpus name %s is not supported\n", corpus_name.c_str());
            return;
        }

        std::string &word = (*corpus)[number(0, corpus->size() - 1)];
        while (word.length() > upper_length) word = (*corpus)[number(0, corpus->size() - 1)];
        strncpy(s, word.c_str(), word.size());
        s[word.size()] = '\0';
    }

    uint32_t RandomGenerator::stockIntDist(const std::string &name) {
        if (name == "ytd") return stock_ytd_dist_[number(0, stock_ytd_dist_.size() - 1)];
        else if (name == "order_cnt")
            return stock_order_cnt_dist_[number(0,
                                                stock_order_cnt_dist_.size() -
                                                1)];
        else if (name == "remote_cnt")
            return stock_remote_cnt_dist_[number(0,
                                                 stock_remote_cnt_dist_.size() -
                                                 1)];
        else {
            printf("Stock num dist name %s is not supported\n", name.c_str());
        }
        return 0;
    }

    uint32_t RandomGenerator::customerIntDist(const std::string &name) {
        if (name == "payment_cnt")
            return cus_payment_cnt_dist_[number(0,
                                                cus_payment_cnt_dist_.size() -
                                                1)];
        else if (name == "delivery_cnt")
            return cus_delivery_cnt_dist_[number(0, cus_delivery_cnt_dist_.size() - 1)];
        else {
            printf("Customer num dist name %s is not supported\n", name.c_str());
        }
        return 0;
    }

    float RandomGenerator::customerFloatDist(const std::string &name) {
        if (name == "balance") return cus_balance_dist_[number(0, cus_balance_dist_.size() - 1)];
        else if (name == "ytd_payment")
            return cus_ytd_payment_dist_[number(0,
                                                cus_ytd_payment_dist_.size() -
                                                1)];
        else {
            printf("Customer float dist name %s is not supported\n", name.c_str());
        }
        return 0;
    }

    // Defined by TPC-C 4.3.2.3.
    void makeLastName(int num, char *name) {
        static const char *const SYLLABLES[] = {
                "BAR", "OUGHT", "ABLE", "PRI", "PRES", "ESE", "ANTI", "CALLY", "ATION", "EING",};
        static const int LENGTHS[] = {3, 5, 4, 3, 4, 3, 4, 5, 5, 4,};

        ASSERT(0 <= num && num <= 999);
        int indicies[] = {num / 100, (num / 10) % 10, num % 10};

        int offset = 0;
        for (int i = 0; i < sizeof(indicies) / sizeof(*indicies); ++i) {
            ASSERT(strlen(SYLLABLES[indicies[i]]) == LENGTHS[indicies[i]]);
            memcpy(name + offset, SYLLABLES[indicies[i]],
                   static_cast<size_t>(LENGTHS[indicies[i]]));
            offset += LENGTHS[indicies[i]];
            name[offset] = '-';
            ++offset;
        }
        name[offset - 1] = '\0';
    }

    RealRandomGenerator::RealRandomGenerator() {
#ifdef HAVE_RANDOM_R
        // Set the random state to zeros. glibc will attempt to access the old state if not NULL.
        memset(&state, 0, sizeof(state));
        int result = initstate_r(static_cast<unsigned int>(time(NULL)), state_array,
                                 sizeof(state_array), &state);
        ASSERT(result == 0);
#else
        seed(time(NULL));
#endif
    }

    int RealRandomGenerator::number(int lower, int upper) {
        int rand_int;
#ifdef HAVE_RANDOM_R
        int error = random_r(&state, &rand_int);
        ASSERT(error == 0);
#else
        rand_int = nrand48(state);
#endif
        ASSERT(0 <= rand_int && rand_int <= RAND_MAX);

        // Select a number in [0, range_size-1]
        int range_size = upper - lower + 1;
        rand_int %= range_size;
        ASSERT(0 <= rand_int && rand_int < range_size);

        // Shift the range to [lower, upper]
        rand_int += lower;
        ASSERT(lower <= rand_int && rand_int <= upper);
        return rand_int;
    }

    void RealRandomGenerator::seed(unsigned int seed) {
#ifdef HAVE_RANDOM_R
        int error = srandom_r(seed, &state);
        ASSERT(error == 0);
#else
        memcpy(state, &seed, std::min(sizeof(seed), sizeof(state)));
#endif
    }

}  // namespace tpcc
