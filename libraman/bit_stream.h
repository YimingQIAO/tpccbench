#pragma once

#include <istream>
#include <ostream>
#include <vector>

class BitStream {
public:
    uint32_t idx_;

    BitStream() : bits_(64) {}

    inline uint32_t size() { return idx_ >> 3; }

    inline void Clear() { num_bits_ = 0; }

    inline bool GetBit(size_t idx) const {
        if (idx >= num_bits_) throw std::logic_error("Index out of bounds");
        size_t byte_pos = idx >> 3;
        uint8_t bit_pos = idx & 7;

        return bits_[byte_pos] & (1 << (7 - bit_pos));
    }

    uint32_t GetLogR(size_t logR) const {
        uint32_t ret = 0;
        for (size_t i = 0; i < logR; ++i) {
            ret = (ret << 1) | GetBit(i);
        }
        return ret;
    }

    BitStream SubStream(size_t start, size_t len) const {
        BitStream stream;
        stream.bits_.resize((len + 7) >> 3);
        for (size_t i = 0; i < len; ++i) stream.WriteBit(GetBit(start + i));
        return stream;
    }

    inline void SetBit(size_t idx, bool val) {
        if (idx >= num_bits_) throw std::logic_error("Index out of bounds");
        size_t byte_pos = idx >> 3;
        uint8_t bit_pos = idx & 7;

        if (val)
            bits_[byte_pos] |= (1 << (7 - bit_pos));
        else
            bits_[byte_pos] &= ~(1 << (7 - bit_pos));
    }

    inline void WriteBit(bool val) {
        if (num_bits_ == bits_.size() * 8) bits_.resize((bits_.size() << 1) + 1);
        SetBit(num_bits_++, val);
    }

    inline void WriteBits(const std::vector<bool> &bits) {
        for (bool bit: bits) WriteBit(bit);
    }

    inline void InitBits(uint32_t delta, size_t n_bits) {
        num_bits_ = 0;

        for (size_t i = 0; i < n_bits; ++i) {
            WriteBit(delta & (1 << (n_bits - i - 1)));
        }
    }

    inline size_t Size() const { return num_bits_; }

    inline bool ReadBit(uint32_t pos) {
        if (pos >= num_bits_) throw std::logic_error("Read past end of stream");
        return GetBit(pos);
    }

    bool operator<(const BitStream &other) const {
        size_t i = 0;
        while (i < num_bits_ && i < other.num_bits_) {
            if (GetBit(i) != other.GetBit(i)) return GetBit(i) < other.GetBit(i);
            i++;
        }
        return num_bits_ < other.num_bits_;
    }

    BitStream operator-(const BitStream &other) const {
        BitStream stream;
        stream.bits_.resize((num_bits_ + 7) >> 3);
        stream.num_bits_ = num_bits_;
        bool carry = false;
        for (int i = num_bits_ - 1; i >= 0; i--) {
            // minus
            stream.SetBit(i, GetBit(i) ^ other.GetBit(i) ^ carry);
            carry = (GetBit(i) && carry) || (GetBit(i) && other.GetBit(i)) || (carry && other.GetBit(i));
        }
        return stream;
    }

    BitStream operator+(const BitStream &other) const {
        BitStream stream;
        stream.bits_.resize((num_bits_ + 7) >> 3);
        stream.num_bits_ = num_bits_;
        bool carry = false;
        for (int i = num_bits_ - 1; i >= 0; i--) {
            // plus
            stream.SetBit(i, GetBit(i) ^ other.GetBit(i) ^ carry);
            carry = (GetBit(i) && other.GetBit(i)) || (carry && other.GetBit(i)) || (carry && GetBit(i));
        }
        return stream;
    }

    BitStream &operator|=(const BitStream &other) {
        if (num_bits_ != other.num_bits_) throw std::logic_error("BitStreams must be the same length");
        for (size_t i = 0; i < num_bits_; ++i) SetBit(i, GetBit(i) | other.GetBit(i));
        return *this;
    }

    BitStream &operator+=(const BitStream &other) {
        for (size_t i = 0; i < other.num_bits_; ++i) WriteBit(other.GetBit(i));
        return *this;
    }

    bool operator!=(const BitStream &other) const {
        if (num_bits_ != other.num_bits_) return true;
        for (size_t i = 0; i < num_bits_; ++i)
            if (GetBit(i) != other.GetBit(i)) return true;
        return false;
    }

private:
    std::vector<uint8_t> bits_;
    size_t num_bits_ = 0;
};
