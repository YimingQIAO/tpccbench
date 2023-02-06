#ifndef RANDOMGENERATOR_H__
#define RANDOMGENERATOR_H__

#include <cstdlib> // for struct random_data
#include <string>
#include <vector>
#include <fstream>
#include <cstring>
#include <sstream>

#ifdef __linux__
#define HAVE_RANDOM_R
#endif

namespace tpcc {

    class RandomGenerator;

// Constant C values for the NURand function.
    struct NURandC {
        NURandC() : c_last_(0), c_id_(0), ol_i_id_(0) {}

        int c_last_;
        int c_id_;
        int ol_i_id_;

        // Sets the fields randomly.
        static NURandC makeRandom(RandomGenerator *generator);

        // Sets the fields randomly, in a fashion acceptable for a test run. c_load is
        // the value of c_last_ that was used to generate the tables. See
        // TPC-C 2.1.6.1. (page 20).
        static NURandC makeRandomForRun(RandomGenerator *generator,
                                        const NURandC &c_load);
    };

    class RandomGenerator {
    public:
        RandomGenerator() : c_values_(NURandC()) {
            loadCorpus();
            loadDataDist();
        }

        virtual ~RandomGenerator() {}

        // Return a random integer in the range [lower, upper]. The range is
        // inclusive.
        virtual int number(int lower, int upper) = 0;

        // Return a random integer in the range [lower, upper] excluding excluded. The
        // range is inclusive.
        int numberExcluding(int lower, int upper, int excluding);

        void astring(char *s, int lower_length, int upper_length, int cardinality);

        void nstring(char *s, int lower_length, int upper_length);

        static void distInfo(char *s, int d_id, int w_id, int i_id);

        // Fill name with a random last name, generated according to TPC-C rules.
        // Limits the customer id for the generated name to cid.
        void lastName(char *name, int max_cid);

        float fixedPoint(int digits, float lower, float upper);

        // Non-uniform random number function from TPC-C 2.1.6. (page 20).
        int NURand(int A, int x, int y);

        int *makePermutation(int lower, int upper);

        void setC(const NURandC &c) { c_values_ = c; }

        void stockData(char *s, int upper_length) {
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

        void customerData(char *s, int upper_length) {
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
        }

        void phoneData(char *s, int length) {
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

        void departmentData(char *s, int upper_length) {
            if (upper_length > 13) {
                memcpy(s, "Department#", 11);
                nstring(s + 11, 1, 2);
            }
        }

        void corpusData(char *s, int upper_length, const std::string &corpus_name) {
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

        uint32_t stockNumDist(const std::string &name) {
            if (name == "ytd") return stock_ytd_dist_[number(0, stock_ytd_dist_.size() - 1)];
            else if (name == "order_cnt") return stock_order_cnt_dist_[number(0, stock_order_cnt_dist_.size() - 1)];
            else if (name == "remote_cnt") return stock_remote_cnt_dist_[number(0, stock_remote_cnt_dist_.size() - 1)];
            else {
                printf("Stock num dist name %s is not supported\n", name.c_str());
            }
            return 0;
        }

    private:
        NURandC c_values_;
        std::vector<std::string> stock_data_corpus_;
        std::vector<std::string> first_names_;
        std::vector<std::string> zip_;
        std::vector<std::string> city_;
        std::vector<std::string> state_;
        std::vector<std::string> street_;
        std::vector<std::string> phone_district_code = {
                "617", "508", // Boston, MA
                "773", "312", "872", // Chicago, IL
                "214", "469", "972", // Dallas, TX
                "303", "720", // Denver, CO
                "305", "786", // Miami, FL
                "212", "646", // New York, NY
                "267", "215", // Philadelphia, PA
                "602", "480", // Phoenix, AZ
                "503", "971", // Portland, OR
                "901", "615", "423", // Memphis, TN
                "210", "512", // San Antonio, TX
                "415", "650", "408", // San Francisco, CA
                "206", "425", // Seattle, WA
                "703", "571", // Washington, DC
        };

        std::vector<uint32_t> stock_ytd_dist_;
        std::vector<uint32_t> stock_order_cnt_dist_;
        std::vector<uint32_t> stock_remote_cnt_dist_;

        void loadCorpus() {
            if (!loadStockDataCorpus()) {
                printf("Loaded stock_data_corpus.txt failed\n");
            }
            if (!loadFirstNames()) {
                printf("Loaded first_names.txt failed\n");
            }
            if (!loadZip()) {
                printf("Loaded zip.txt failed\n");
            }
            if (!loadStreet()) {
                printf("Loaded street.txt failed\n");
            }
        }

        bool loadStockDataCorpus() {
            std::ifstream in("corpus/stock_data_corpus.txt");
            if (in.fail()) {
                printf("Failed to open stock_data_corpus.txt\n");
                return false;
            }

            std::string line;
            while (std::getline(in, line)) {
                if (line.back() == '\r') line.pop_back();
                stock_data_corpus_.push_back(line);
            }
            in.close();
            return true;
        }

        bool loadFirstNames() {
            std::ifstream in("corpus/first_names.txt");
            if (in.fail()) {
                printf("Failed to open first_names.txt\n");
                return false;
            }

            std::string line;
            while (std::getline(in, line)) {
                if (line.back() == '\r') line.pop_back();
                first_names_.push_back(line);
            }
            in.close();
            return true;
        }

        bool loadZip() {
            std::ifstream in("corpus/zip_corpus.txt");
            if (in.fail()) {
                printf("Failed to open zip.txt\n");
                return false;
            }

            std::string line;
            while (std::getline(in, line)) {
                if (line.back() == '\r') line.pop_back();
                const int32_t zip_length = 5;
                const int32_t state_length = 2;
                zip_.push_back(line.substr(0, zip_length));
                state_.push_back(line.substr(line.size() - state_length, state_length));
                city_.push_back(line.substr(zip_length + 1, line.size() - zip_length - state_length - 2));
            }
            in.close();
            return true;
        }

        bool loadStreet() {
            std::ifstream in("corpus/streets.txt");
            if (in.fail()) {
                printf("Failed to open street.txt\n");
                return false;
            }

            std::string line;
            while (std::getline(in, line)) {
                if (line.back() == '\r') line.pop_back();
                street_.push_back(line);
            }
            in.close();
            return true;
        }

        bool loadDataDist() {
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
            return true;
        }
    };

// A mock RandomGenerator for unit testing.
    class MockRandomGenerator : public RandomGenerator {
    public:
        MockRandomGenerator() : minimum_(true) {}

        virtual int number(int lower, int upper) {
            if (minimum_)
                return lower;
            else
                return upper;
        }

        bool minimum_;
    };

    static const int MAX_LAST_NAME = 16;

// Generate a last name as defined by TPC-C 4.3.2.3. name must be at least
// MAX_LAST_NAME+1 bytes.
    void makeLastName(int num, char *name);

// A real RandomGenerator that uses random_r.
    class RealRandomGenerator : public RandomGenerator {
    public:
        // Seeds the generator with the current time.
        RealRandomGenerator();

        virtual int number(int lower, int upper);

        // Seed the generator with seed.
        void seed(unsigned int seed);

    private:
#ifdef HAVE_RANDOM_R
        // man random says optimal sizes are 8, 32, 64, 128, 256 bytes
        static const int RANDOM_STATE_SIZE = 64;
        char state_array[RANDOM_STATE_SIZE];
        struct random_data state;
#else
        unsigned short state[3];
#endif
    };

} // namespace tpcc

#endif
