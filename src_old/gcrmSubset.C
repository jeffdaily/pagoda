#include <math.h>

#include <iostream>
  using std::cout;
  using std::endl;
#include <set>
  using std::set;
#include <string>
  using std::string;
#include <utility>
  using std::make_pair;
  using std::pair;
#include <vector>
  using std::vector;

#include <boost/algorithm/string.hpp>
#include <boost/lexical_cast.hpp>
#include <boost/program_options.hpp>
#include <boost/regex.hpp>
  using namespace boost;
  using namespace boost::program_options;

#include <netcdfcpp.h>


// global vars, yuck
vector< pair<int,int> > levels;
vector< pair<int,int> > cellIndices;


vector< pair<int,int> > getLevels(const vector<string>& levels);
int getSize(const vector< pair<int,int> >& indices);
template <class T>
void copyOneDim(NcVar *varIn, NcVar *varOut);
template <class T>
void copyTwoDim(NcVar *varIn, NcVar *varOut);
template <class T>
void copyThreeDim(NcVar *varIn, NcVar *varOut);
template <class T>
void copyThreeDimLowMem(NcVar *varIn, NcVar *varOut);
template <class T>
void copyVar(NcVar *varIn, NcVar *varOut, long *cornerIn, long *cornerOut,
             long *counts, vector< pair<int,int> > *indices, int dimIdx);


int main(int argc, char **argv)
{
  // Declare the supported options
  options_description options("Allowed options");
  options.add_options()
    ("help,h",
     "produce help message")
    ("north,n",
     value<double>()->default_value(91),
     "north limit in degress")
    ("south,s",
     value<double>()->default_value(-91.0),
     "south limit in degress")
    ("east,e",
     value<double>()->default_value(181.0),
     "east limit in degress")
    ("west,w",
     value<double>()->default_value(-181.0),
     "west limit in degress")
    ("level,l",
     value< vector<string> >(),
     "level")
    ("input-file,i",
     value< vector<string> >(),
     "input file")
    ("output-file,o",
     value<string>(),
     "output file")
  ;
  positional_options_description positional;
  positional.add("input-file", -1);

  variables_map vm;
  store(command_line_parser(argc, argv)
          .options(options).positional(positional).run(), vm);
  notify(vm);

  if (vm.count("help")) {
    cout << options << endl;
    return EXIT_SUCCESS;
  }

  if (vm.count("level")) {
    levels = getLevels(vm["level"].as< vector<string> >());
  } else {
    levels = vector< pair<int,int> >(1, make_pair(-1,-1));
  }

  if (!vm.count("input-file")) {
    cout << "input-file parameter(s) needed" << endl;
    return EXIT_FAILURE;
  }

  // convert the lat/lon bounds to radians from degrees
  double latMax = vm["north"].as<double>();
  double latMin = vm["south"].as<double>();
  double lonMax = vm["east"].as<double>();
  double lonMin = vm["west"].as<double>();
  double degToRad = M_PI / 180.0;
  latMax *= degToRad;
  latMin *= degToRad;
  lonMax *= degToRad;
  lonMin *= degToRad;

  // now subset the netcdf files, one at a time

  vector<string> inputFileNames = vm["input-file"].as< vector<string> >();
  vector<string>::const_iterator inputFileName;
  for (inputFileName = inputFileNames.begin();
          inputFileName != inputFileNames.end();
          ++inputFileName) {
    string outputFileName =
              inputFileName->substr(0, inputFileName->size() - 3) + "_sub.nc";

    NcFile infile(inputFileName->c_str(), NcFile::ReadOnly);
    if (!infile.is_valid()) {
      cout << "Couldn't open file" << endl;
      return EXIT_FAILURE;
    }

    // get references to all needed parts of input file for subsetting region
    NcVar *grid_center_lat = infile.get_var("grid_center_lat");
    NcVar *grid_center_lon = infile.get_var("grid_center_lon");
    int grid_cells_size = infile.get_dim("grid_cells")->size();

    // let's try to read the entire cell center contents into memory
    float *grid_center_lat_data = new float[grid_cells_size];
    float *grid_center_lon_data = new float[grid_cells_size];
    grid_center_lat->get(&grid_center_lat_data[0], grid_cells_size);
    grid_center_lon->get(&grid_center_lon_data[0], grid_cells_size);

    /*
    // convert to degress from radians
    double radToDeg = 180.0 / M_PI;
    for (int i = 0; i < grid_cells_size; ++i) {
      grid_center_lat_data[i] = radToDeg * grid_center_lat_data[i];
      grid_center_lon_data[i] = radToDeg * grid_center_lon_data[i];
    }
    */

    // calculate indices based on grid centers
    int i;
    cellIndices.clear();
    pair<int,int> lastIndex;
    for (i = 0; i < grid_cells_size; ++i) {
      if (grid_center_lat_data[i] <= latMax
              && grid_center_lat_data[i] >= latMin
              && grid_center_lon_data[i] <= lonMax
              && grid_center_lon_data[i] >= lonMin)
      {
        lastIndex = make_pair(i,i);
        break;
      }
    }
    for (i = i + 1; i < grid_cells_size; ++i) {
      if (grid_center_lat_data[i] <= latMax
              && grid_center_lat_data[i] >= latMin
              && grid_center_lon_data[i] <= lonMax
              && grid_center_lon_data[i] >= lonMin)
      {
        if (i - lastIndex.second == 1) {
          ++lastIndex.second;
        } else {
          cellIndices.push_back(lastIndex);
          lastIndex = make_pair(i,i);
        }
      }
    }
    cellIndices.push_back(lastIndex);

    // begin copying of data from infile to outfile
    NcFile outfile(outputFileName.c_str(), NcFile::Replace);
    // first, define the dimensions
    for (int i = 0; i < infile.num_dims(); ++i) {
      NcDim *dim = infile.get_dim(i);
      string name = dim->name();
      if (name == "grid_cells") {
        outfile.add_dim(dim->name(), getSize(cellIndices));
      } else if (name == "level") {
        if (levels[0] == make_pair(-1,-1)) {
          outfile.add_dim(dim->name(), dim->size());
        } else {
          outfile.add_dim(dim->name(), getSize(levels));
        }
      } else if (dim->is_unlimited()) {
        outfile.add_dim(dim->name());
      } else {
        outfile.add_dim(dim->name(), dim->size());
      }
    }
    // next, define the variables
    for (int i = 0; i < infile.num_vars(); ++i) {
      NcVar *var = infile.get_var(i);
      const NcDim **dims = new const NcDim*[var->num_dims()];
      for (int j = 0; j < var->num_dims(); ++j) {
        dims[j] = outfile.get_dim(var->get_dim(j)->name());
      }
      outfile.add_var(var->name(), var->type(), var->num_dims(), dims);
      delete[] dims;
    }

    // now that outfile is well defined, start writing data to it
    for (int i = 0; i < infile.num_vars(); ++i) {
      NcVar *varIn = infile.get_var(i);
      NcVar *varOut = outfile.get_var(varIn->name());
      if (varOut->num_dims() == 1) {
        switch(varOut->type()) {
          case ncByte:
            break;
          case ncChar:
            copyOneDim<char>(varIn, varOut);
            break;
          case ncShort:
            break;
          case ncInt:
            break;
          case ncFloat:
            copyOneDim<float>(varIn, varOut);
            break;
          case ncDouble:
            copyOneDim<double>(varIn, varOut);
            break;
        }
      } else if (varOut->num_dims() == 2) {
        switch(varOut->type()) {
          case ncByte:
            break;
          case ncChar:
            copyTwoDim<char>(varIn, varOut);
            break;
          case ncShort:
            break;
          case ncInt:
            break;
          case ncFloat:
            copyTwoDim<float>(varIn, varOut);
            break;
          case ncDouble:
            copyTwoDim<double>(varIn, varOut);
            break;
        }
      } else if (varOut->num_dims() == 3) {
        switch(varOut->type()) {
          case ncByte:
            break;
          case ncChar:
            copyThreeDim<char>(varIn, varOut);
            break;
          case ncShort:
            break;
          case ncInt:
            break;
          case ncFloat:
            copyThreeDim<float>(varIn, varOut);
            break;
          case ncDouble:
            copyThreeDim<double>(varIn, varOut);
            break;
        }
      }
    }

#ifdef RECURSIVE
    for (int i = 0; i < infile.num_vars(); ++i) {
      NcVar *varIn = infile.get_var(i);
      NcVar *varOut = outfile.get_var(varIn->name());
      int numDims = varIn->num_dims();
      long *counts = new long[numDims];
      long *cornerIn = new long[numDims];
      long *cornerOut = new long[numDims];
      vector< pair<int,int> > *indices = new vector< pair<int,int> >[numDims];
      for (int dimIdx = 0; dimIdx < numDims; ++dimIdx) {
        string dimName = varIn->get_dim(dimIdx)->name();
        if (dimName == "level" && levels[0] != make_pair(-1,-1)) {
          indices[dimIdx] = levels;
        } else if (dimName == "grid_cells") {
          indices[dimIdx] = cellIndices;
        } else {
          indices[dimIdx] = vector< pair<int,int> >(1,
                  make_pair(0, varIn->get_dim(dimIdx)->size() - 1));
        }
        counts[dimIdx] = 0;
        cornerIn[dimIdx] = 0;
        cornerOut[dimIdx] = 0;
      }
      switch(varIn->type()) {
        case ncByte:
          break;
        case ncChar:
          break;
        case ncShort:
          break;
        case ncInt:
          break;
        case ncFloat:
          copyVar<float>(varIn, varOut, cornerIn, cornerOut,
                         counts, indices, 0);
          break;
        case ncDouble:
          copyVar<double>(varIn, varOut, cornerIn, cornerOut,
                          counts, indices, 0);
          break;

      }
    }
#endif // RECURSIVE
  }

  
  return EXIT_SUCCESS;
}


vector< pair<int,int> > getLevels(const vector<string>& levels)
{
  // 2,3,4,6-10,12-15,76
  // 2
  // 2-10
  // 2,5
  static regex r("^((\\d+|\\d+-\\d+),)*(\\d+|\\d+-\\d+)$");

  // parse level strings and add to set
  vector< pair<int,int> > retVec;
  vector<string>::const_iterator levelVecIt;
  for (levelVecIt = levels.begin(); levelVecIt != levels.end(); ++levelVecIt) {
    // Do regex match to make sure input is valid
    smatch match;
    if (regex_match(*levelVecIt, match, r)) {
      vector<string> parts;
      split(parts, *levelVecIt, is_any_of(","));
      vector<string>::iterator part;
      for (part = parts.begin(); part != parts.end(); part++) {
        vector<string> moreParts;
        split(moreParts, *part, is_any_of("-"));
        int first = lexical_cast<int>(moreParts[0]);
        int second = moreParts.size() > 1 ? lexical_cast<int>(moreParts[1]) :
                                            first;
        retVec.push_back(make_pair(first,second));
      }
    }
    else {
      throw "Error parsing level argument";
    }
  }

  // sort the parsed levels
  sort(retVec.begin(), retVec.end());

  // condense the vector
  vector< pair<int,int> >::iterator currentLevel = ++(retVec.begin());
  vector< pair<int,int> >::iterator lastLevel = retVec.begin();
  while (currentLevel != retVec.end()) {
    if ((currentLevel->first < lastLevel->second)
            || currentLevel->first == (lastLevel->second + 1)) {
      if (currentLevel->second > lastLevel->second) {
        lastLevel->second = currentLevel->second;
      }
      currentLevel = retVec.erase(currentLevel);
    } else {
      ++currentLevel;
      ++lastLevel;
    }
  }

  return retVec;
}


int getSize(const vector< pair<int,int> >& indices)
{
  int ret = 0;
  vector< pair<int,int> >::const_iterator index;
  for (index = indices.begin(); index != indices.end(); ++index) {
    ret += index->second - index->first + 1;
  }
  return ret;
}


template <class T>
void copyOneDim(NcVar *inVar, NcVar *outVar)
{
  // assume we have enough memory to hold a single-dimensioned variable
  // also assume inVar and outVar match in dimension count name
  NcDim *dim = outVar->get_dim(0);
  string name = dim->name();

  T* data = new T[dim->size()];
  T* data_head = data;

  vector< pair<int,int> > indices;
  if (name == "level" && levels[0] != make_pair(-1,-1)) {
    indices = levels;
  } else if (name == "grid_cells") {
    indices = cellIndices;
  } else {
    indices.push_back(make_pair(0,dim->size()-1));
  }

  vector< pair<int,int> >::iterator index;
  for (index = indices.begin(); index != indices.end(); ++index) {
    long edge = index->second - index->first + 1;
    inVar->set_cur(index->first);
    inVar->get(data, edge);
    data += edge;
  }

  outVar->put(data_head, dim->size());
  delete data_head;
}


template <class T>
void copyTwoDim(NcVar *inVar, NcVar *outVar)
{
  // assume we have enough memory to hold a two-dimensional variable

  NcDim *dimOut0 = outVar->get_dim(0);
  NcDim *dimOut1 = outVar->get_dim(1);
  NcDim *dimIn0 = inVar->get_dim(0);
  NcDim *dimIn1 = inVar->get_dim(1);

  string name0 = dimOut0->name();
  string name1 = dimOut1->name();

  T *data = new T[dimOut0->size() * dimOut1->size()];
  T *data_head = data;

  vector< pair<int,int> > index0;
  vector< pair<int,int> > index1;

  if (name0 == "level" && levels[0] != make_pair(-1,-1)) {
    index0 = levels;
  } else if (name0 == "grid_cells") {
    index0 = cellIndices;
  } else {
    index0.push_back(make_pair(0, dimIn0->size()-1));
  }

  if (name1 == "level" && levels[0] != make_pair(-1,-1)) {
    index1 = levels;
  } else if (name1 == "grid_cells") {
    index1 = cellIndices;
  } else {
    index1.push_back(make_pair(0, dimIn1->size()-1));
  }

  long edge0, edge1;
  vector< pair<int,int> >::iterator it0, it1;

  for (it0 = index0.begin(); it0 != index0.end(); ++it0) {
    edge0 = it0->second - it0->first + 1;
    for (it1 = index1.begin(); it1 != index1.end(); ++it1) {
      edge1 = it1->second - it1->first + 1;
      inVar->set_cur(it0->first, it1->first);
      inVar->get(data, edge0, edge1);
      data += edge0 * edge1;
    }
  }

  outVar->put(data_head, dimOut0->size(), dimOut1->size());
  delete data_head;
}


template <class T>
void copyThreeDim(NcVar *inVar, NcVar *outVar)
{
  NcDim *dimOut0 = outVar->get_dim(0);
  NcDim *dimOut1 = outVar->get_dim(1);
  NcDim *dimOut2 = outVar->get_dim(2);
  NcDim *dimIn0 = inVar->get_dim(0);
  NcDim *dimIn1 = inVar->get_dim(1);
  NcDim *dimIn2 = inVar->get_dim(2);

  string name0 = dimOut0->name();
  string name1 = dimOut1->name();
  string name2 = dimOut2->name();

  long dim0Size = dimOut0->is_unlimited() ? dimIn0->size() : dimOut0->size();
  long dim1Size = dimOut1->size();
  long dim2Size = dimOut2->size();
  long dimSize = dim0Size * dim1Size * dim2Size;

  if (dimSize >= 1048576200) {
    // 1 timestep, 100 levels, 10485762 cells (R=10)
    // otherwise all time spent thrashing swap space
    copyThreeDimLowMem<T>(inVar, outVar);
    return;
  }

  T *data = new T[dimSize];
  T *data_head = data;

  vector< pair<int,int> > index0;
  vector< pair<int,int> > index1;
  vector< pair<int,int> > index2;

  if (name0 == "level" && levels[0] != make_pair(-1,-1)) {
    index0 = levels;
  } else if (name0 == "grid_cells") {
    index0 = cellIndices;
  } else {
    index0.push_back(make_pair(0, dim0Size-1));
  }

  if (name1 == "level" && levels[0] != make_pair(-1,-1)) {
    index1 = levels;
  } else if (name1 == "grid_cells") {
    index1 = cellIndices;
  } else {
    index1.push_back(make_pair(0, dim1Size-1));
  }

  if (name2 == "level" && levels[0] != make_pair(-1,-1)) {
    index2 = levels;
  } else if (name2 == "grid_cells") {
    index2 = cellIndices;
  } else {
    index2.push_back(make_pair(0, dim2Size-1));
  }

  long edge0, edge1, edge2;
  vector< pair<int,int> >::iterator it0, it1, it2;

  for (it0 = index0.begin(); it0 != index0.end(); ++it0) {
    edge0 = it0->second - it0->first + 1;
    for (it1 = index1.begin(); it1 != index1.end(); ++it1) {
      edge1 = it1->second - it1->first + 1;
      for (it2 = index2.begin(); it2 != index2.end(); ++it2) {
        edge2 = it2->second - it2->first + 1;
        inVar->set_cur(it0->first, it1->first, it2->first);
        inVar->get(data, edge0, edge1, edge2);
        data += edge0 * edge1 * edge2;
      }
    }
  }

  outVar->put(data_head, dim0Size, dim1Size, dim2Size);
  delete data_head;
}


template <class T>
void copyThreeDimLowMem(NcVar *inVar, NcVar *outVar)
{
  // the strategy here is that we'll only use as much memory as needed by the 
  // last dimension.  the reason for needing this special function is
  // for handling say "pressure(time, level, grid_cells)" where grid_cells
  // is 41943042 in size (R=11).

  NcDim *dimOut0 = outVar->get_dim(0);
  NcDim *dimOut1 = outVar->get_dim(1);
  NcDim *dimOut2 = outVar->get_dim(2);
  NcDim *dimIn0 = inVar->get_dim(0);
  NcDim *dimIn1 = inVar->get_dim(1);
  NcDim *dimIn2 = inVar->get_dim(2);

  string name0 = dimOut0->name();
  string name1 = dimOut1->name();
  string name2 = dimOut2->name();

  long dim0Size = dimOut0->is_unlimited() ? dimIn0->size() : dimOut0->size();
  long dim1Size = dimOut1->size();
  long dim2Size = dimOut2->size();

  T *data = new T[dim2Size];
  T *data_head = data;

  vector< pair<int,int> > index0;
  vector< pair<int,int> > index1;
  vector< pair<int,int> > index2;

  if (name0 == "level" && levels[0] != make_pair(-1,-1)) {
    index0 = levels;
  } else if (name0 == "grid_cells") {
    index0 = cellIndices;
  } else {
    index0.push_back(make_pair(0, dim0Size-1));
  }

  if (name1 == "level" && levels[0] != make_pair(-1,-1)) {
    index1 = levels;
  } else if (name1 == "grid_cells") {
    index1 = cellIndices;
  } else {
    index1.push_back(make_pair(0, dim1Size-1));
  }

  if (name2 == "level" && levels[0] != make_pair(-1,-1)) {
    index2 = levels;
  } else if (name2 == "grid_cells") {
    index2 = cellIndices;
  } else {
    index2.push_back(make_pair(0, dim2Size-1));
  }

  long edge0 = 0, edge1 = 0, edge2 = 0;
  vector< pair<int,int> >::iterator it0, it1, it2;

  for (it0 = index0.begin(); it0 != index0.end(); ++it0) {
    for (int idx0 = it0->first; idx0 <= it0->second; ++idx0) {
      for (it1 = index1.begin(); it1 != index1.end(); ++it1) {
        for (int idx1 = it1->first; idx1 <= it1->second; ++idx1) {
          for (it2 = index2.begin(); it2 != index2.end(); ++it2) {
            edge2 = it2->second - it2->first + 1;
            inVar->set_cur(idx0, idx1, it2->first);
            inVar->get(data, 1, 1, edge2);
            data += edge2;
          }
          outVar->set_cur(edge0,edge1,0);
          outVar->put(data_head, 1, 1, dim2Size);
          ++edge1; // level
        }
      }
      ++edge0; // time
    }
  }

  outVar->put(data_head, dim0Size, dim1Size, dim2Size);
  delete data_head;
}


template <class T>
void copyVar(NcVar *varIn, NcVar *varOut, long *cornerIn, long *cornerOut,
             long *counts, vector< pair<int,int> > *indices, int dimIdx)
{
  if (dimIdx >= varIn->num_dims()) {
    long countsTotal = 1;
    for (int i = 0; i < varIn->num_dims(); ++i) {
      countsTotal *= counts[i];
    }
    T *data = new T[countsTotal];
    varIn->set_cur(cornerIn);
    varIn->get(data, counts);
    varOut->set_cur(cornerOut);
    varOut->put(data, counts);
    delete data;
  } else {
    vector< pair<int,int> >::iterator it;
    for (it = indices[dimIdx].begin(); it != indices[dimIdx].end(); ++it) {
      cornerIn[dimIdx] = it->first;
      counts[dimIdx] = it->second - it->first + 1;
      copyVar<T>(varIn, varOut, cornerIn, cornerOut, counts, indices, dimIdx+1);
      cornerOut[dimIdx] += counts[dimIdx];
      if (cornerOut[dimIdx] >= varOut->get_dim(dimIdx)->size()) {
        cornerOut[dimIdx] = 0;
      }
    }
  }
}

