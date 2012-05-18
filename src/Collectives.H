#ifndef COLLECTIVES_H_
#define COLLECTIVES_H_

#include <stdint.h>

#include <vector>

using std::vector;

/**
 * Holds miscellanous functionality.
 */
namespace pagoda
{
    void barrier();
    void gop_sum(vector<char> &values);
    void gop_sum(vector<short> &values);
    void gop_sum(vector<int> &values);
    void gop_sum(vector<long> &values);
    void gop_sum(vector<long long> &values);
#if NEED_VECTOR_INT64_T_GOP
    void gop_sum(vector<int64_t> &values);
#endif
    void gop_sum(vector<float> &values);
    void gop_sum(vector<double> &values);
    void gop_sum(vector<long double> &values);
    void gop_sum(vector<unsigned char> &values);
    void gop_sum(vector<unsigned short> &values);
    void gop_sum(vector<unsigned int> &values);
    void gop_sum(vector<unsigned long> &values);
    void gop_sum(vector<unsigned long long> &values);
    void gop_sum(char &value);
    void gop_sum(short &value);
    void gop_sum(int &value);
    void gop_sum(long &value);
    void gop_sum(long long &value);
#if NEED_VECTOR_INT64_T_GOP
    void gop_sum(int64_t &value);
#endif
    void gop_sum(float &value);
    void gop_sum(double &value);
    void gop_sum(long double &value);
    void gop_sum(unsigned char &value);
    void gop_sum(unsigned short &value);
    void gop_sum(unsigned int &value);
    void gop_sum(unsigned long &value);
    void gop_sum(unsigned long long &value);

    void gop_min(vector<char> &values);
    void gop_min(vector<short> &values);
    void gop_min(vector<int> &values);
    void gop_min(vector<long> &values);
    void gop_min(vector<long long> &values);
#if NEED_VECTOR_INT64_T_GOP
    void gop_min(vector<int64_t> &values);
#endif
    void gop_min(vector<float> &values);
    void gop_min(vector<double> &values);
    void gop_min(vector<long double> &values);
    void gop_min(vector<unsigned char> &values);
    void gop_min(vector<unsigned short> &values);
    void gop_min(vector<unsigned int> &values);
    void gop_min(vector<unsigned long> &values);
    void gop_min(vector<unsigned long long> &values);
    void gop_min(char &value);
    void gop_min(short &value);
    void gop_min(int &value);
    void gop_min(long &value);
    void gop_min(long long &value);
#if NEED_VECTOR_INT64_T_GOP
    void gop_min(int64_t &value);
#endif
    void gop_min(float &value);
    void gop_min(double &value);
    void gop_min(long double &value);
    void gop_min(unsigned char &value);
    void gop_min(unsigned short &value);
    void gop_min(unsigned int &value);
    void gop_min(unsigned long &value);
    void gop_min(unsigned long long &value);

    void gop_max(vector<char> &values);
    void gop_max(vector<short> &values);
    void gop_max(vector<int> &values);
    void gop_max(vector<long> &values);
    void gop_max(vector<long long> &values);
#if NEED_VECTOR_INT64_T_GOP
    void gop_max(vector<int64_t> &values);
#endif
    void gop_max(vector<float> &values);
    void gop_max(vector<double> &values);
    void gop_max(vector<long double> &values);
    void gop_max(vector<unsigned char> &values);
    void gop_max(vector<unsigned short> &values);
    void gop_max(vector<unsigned int> &values);
    void gop_max(vector<unsigned long> &values);
    void gop_max(vector<unsigned long long> &values);
    void gop_max(char &value);
    void gop_max(short &value);
    void gop_max(int &value);
    void gop_max(long &value);
    void gop_max(long long &value);
#if NEED_VECTOR_INT64_T_GOP
    void gop_max(int64_t &value);
#endif
    void gop_max(float &value);
    void gop_max(double &value);
    void gop_max(long double &value);
    void gop_max(unsigned char &value);
    void gop_max(unsigned short &value);
    void gop_max(unsigned int &value);
    void gop_max(unsigned long &value);
    void gop_max(unsigned long long &value);

    void broadcast(vector<char> &values, int root);
    void broadcast(vector<short> &values, int root);
    void broadcast(vector<int> &values, int root);
    void broadcast(vector<long> &values, int root);
    void broadcast(vector<long long> &values, int root);
    void broadcast(vector<float> &values, int root);
    void broadcast(vector<double> &values, int root);
    void broadcast(vector<long double> &values, int root);
    void broadcast(vector<unsigned char> &values, int root);
    void broadcast(vector<unsigned short> &values, int root);
    void broadcast(vector<unsigned int> &values, int root);
    void broadcast(vector<unsigned long> &values, int root);
    void broadcast(vector<unsigned long long> &values, int root);
    void broadcast(char &value, int root);
    void broadcast(short &value, int root);
    void broadcast(int &value, int root);
    void broadcast(long &value, int root);
    void broadcast(long long &value, int root);
    void broadcast(float &value, int root);
    void broadcast(double &value, int root);
    void broadcast(long double &value, int root);
    void broadcast(unsigned char &value, int root);
    void broadcast(unsigned short &value, int root);
    void broadcast(unsigned int &value, int root);
    void broadcast(unsigned long &value, int root);
    void broadcast(unsigned long long &value, int root);
    bool file_exists(const string &filename);
} // namespace pagoda

#endif // COLLECTIVES_H_
