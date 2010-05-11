#ifdef HAVE_CONFIG_H
#include "config.h"
#endif // HAVE_CONFIG_H

// C includes, std and otherwise
#include <ga.h>
#include <macdecls.h>
#include <mpi.h>

// C++ includes, std and otherwise
#include <iomanip>
using std::setw;
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
#include "Pack.H"
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

    int ndim = 3;
    int64_t *dims1 = new int64_t[ndim];
    int64_t *result1 = new int64_t[ndim];
    vector<size_t> dims2;
    vector<size_t> result2;
    dims1[0] = 2; dims2.push_back(2);
    dims1[1] = 3; dims2.push_back(3);
    dims1[2] = 4; dims2.push_back(4);

    //cout << "dims=" << dims2 << endl;
    //printf("dims=[%lld,%lld,%lld]\n", dims1[0], dims1[1], dims1[2]);
    for (size_t i=0; i<26; ++i) {
        unravel64(i, 3, dims1, result1);
        printf("unravel64(%zd, %d, [%ld,%ld,%ld], [%ld,%ld,%ld])\n",
                i, ndim,
                (long)dims1[0], (long)dims1[1], (long)dims1[2],
                (long)result1[0], (long)result1[1], (long)result1[2]);
        //result2 = Util::unravel_index(i, dims2);
        //cout << setw(2) << i << " --> " << result2 << " ";
        //printf("[%lld,%lld,%lld]", result1[0], result1[1], result1[2]);
        //cout << " --> " << setw(2) << Util::ravel_index(result2, dims2) << endl;
    }

    delete [] dims1;
    delete [] result1;
    // Must always call these to exit cleanly.
    GA_Terminate();
    MPI_Finalize();

    return EXIT_SUCCESS;
}

