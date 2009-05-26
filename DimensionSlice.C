#include <sstream>
  using std::istringstream;
#include <string>
  using std::string;
#include <vector>
  using std::vector;

#include "DimensionSlice.H"
#include "RangeException.H"


DimensionSlice::DimensionSlice(string arg)
  : name("")
  , start(0)
  , stop(NULL)
  , step(NULL)
{
  vector<string> parts;
  istringstream ss(arg);
  string token;
  while (!ss.eof()) {
    getline(ss, token, ',');
    parts.push_back(token);
  }

  if (parts.size() < 2 or parts.size() > 4) {
    throw RangeException("invalid dimension string");
  }

  if (parts.size() > 3) step = new long(strtol(parts[3].c_str(), NULL, 10));
  if (parts.size() > 2) stop = new long(strtol(parts[2].c_str(), NULL, 10));
  start = strtol(parts[1].c_str(), NULL, 10);
  name = parts[0];
}


DimensionSlice::DimensionSlice(string name, long start, long stop, long step)
  : name(name)
  , start(start)
  , stop(new long(stop))
  , step(new long(step))
{
}


DimensionSlice::DimensionSlice(const DimensionSlice& slice)
  : name(slice.name)
  , start(slice.start)
  , stop(slice.stop == NULL ? NULL : new long(*slice.stop))
  , step(slice.step == NULL ? NULL : new long(*slice.step))
{
}


DimensionSlice::~DimensionSlice()
{
  if (stop) delete stop;
  if (step) delete step;
}


/**
 * Calculate and return the real values for start, stop, and step given size.
 *
 * This will convert any missing stop or step value to an appropriate long
 * and convert negative values into positive values although "step" should be
 * unchanged.
 *
 * Examples:
 * "dim,1,5,2", size 20 --> 1,5,2
 * "dim,1,2", size 20 --> 1,2,1
 * "dim,7", size 20 --> 7,8,1
 * "dim,-10,-20,-1", size 20 --> 10,0,-1
 * "dim,-1", size 20 --> 19,20,1
 */
void DimensionSlice::indices(long size, long &start, long &stop, long &step)
{
  if (size < 0) throw RangeException("size must be positive");

  start = this->start;
  if (start < 0) start += size;
  if (start < 0 || start > size)
    throw RangeException("start < 0 || start > size");

  if (this->stop) {
    stop = *(this->stop);
    if (stop < 0) stop += size;
    if (stop < 0 || stop > size)
      throw RangeException("stop < 0 || stop > size");
  } else {
    stop = start + 1;
  }

  if (this->step) {
    step = *(this->step);
  } else {
    step = 1;
  }

  if (start > stop && step > 0)
    throw RangeException("start > stop && step > 0 (causes no-op loop)");
  if (start < stop && step < 0)
    throw RangeException("start < stop && step < 0 (causes infinite loop)");
}

