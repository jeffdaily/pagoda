#ifdef HAVE_CONFIG_H
#include "config.h"
#endif // HAVE_CONFIG_H

// C includes, std and otherwise
#include <ga.h>
#include <macdecls.h>
#include <mpi.h>

// C++ includes, std and otherwise
#include <iostream>
using std::cout;
using std::endl;
using std::ostream;
#include <string>
using std::string;
#include <vector>
using std::vector;

// C++ includes
#include "AggregationUnion.H"
#include "Attribute.H"
#include "Dimension.H"
#include "NetcdfDataset.H"
#include "Util.H"
#include "Variable.H"


ostream& operator << (ostream& os, const vector<size_t> &vec)
{
    os << "(";
    vector<size_t>::const_iterator iter;
    vector<size_t>::const_iterator end = (vec.end() - 1);
    for (iter=vec.begin(); iter!=end; ++iter) {
        os << (*iter) << ",";
    }
    os << vec.back() << ")";
    return os;
}


int main(int argc, char **argv)
{
    MPI_Init(&argc, &argv);
    GA_Initialize();

    vector<size_t> dims;
    dims.push_back(2);
    dims.push_back(3);
    dims.push_back(4);

    cout << "dims=" << dims << endl;
    for (size_t i=0; i<26; ++i) {
        vector<size_t> result = Util::unravel_index(i, dims);
        size_t result2 = Util::ravel_index(result, dims);
        cout << i << " --> " << result << " --> " << result2 << endl;
    }

    // Must always call these to exit cleanly.
    GA_Terminate();
    MPI_Finalize();

    return EXIT_SUCCESS;
}

