#ifndef DIMENSIONSLICE_H_
#define DIMENSIONSLICE_H_

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
class DimensionSlice
{
  public:
    DimensionSlice(string arg);
    DimensionSlice(string name, long start, long stop, long step);
    DimensionSlice(const DimensionSlice& slice);
    ~DimensionSlice();

    void indices(long size, long &start, long &stop, long &step);

    string name;
    long start;
    long *stop;
    long *step;
};

#endif // DIMENSIONSLICE_H_
