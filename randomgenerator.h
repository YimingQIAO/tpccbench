#ifndef RANDOMGENERATOR_H__
#define RANDOMGENERATOR_H__

#include <cstdlib> // for struct random_data
#include <string>
#include <vector>
#include <fstream>
#include <cstring>

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
            loadStockDataCorpus();
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

    private:
        NURandC c_values_;
        std::vector<std::string> stock_data_corpus_;

        bool loadStockDataCorpus() {
            std::ifstream in("corpus/stock_data_corpus.txt");
            if (in.fail()) {
                printf("Failed to open stock_data_corpus.txt\n");
                return false;
            }

            std::string line;
            while (std::getline(in, line)) {
                line.pop_back();
                stock_data_corpus_.push_back(line);
            }
            in.close();
            // printf("Loaded %zu lines from stock_data_corpus.txt\n", stock_data_corpus_.size());
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
