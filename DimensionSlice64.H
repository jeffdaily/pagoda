#ifndef DIMENSIONSLICE64_H_
#define DIMENSIONSLICE64_H_

#include <string>
  using std::string;

/**
 * Eerily similar to Python's slice class.
 *
 * A slice is indicated by a start, stop, and step value. Any or all of those
 * values are allowed to be negative. The indices function is useful for
 * calculating the real index values for each of start, stop, and step given
 * the size of the dimension being sliced.
 */
class DimensionSlice64
{
  public:
    DimensionSlice64(string arg);
    DimensionSlice64(string name, int64_t start, int64_t stop, int64_t step);
    DimensionSlice64(const DimensionSlice64& slice);
    ~DimensionSlice64();

    void indices(int64_t size, int64_t &start, int64_t &stop, int64_t &step);

    string name;
    int64_t start;
    int64_t *stop;
    int64_t *step;
};

#endif // DIMENSIONSLICE64_H_
