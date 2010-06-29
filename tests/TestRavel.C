#ifdef HAVE_CONFIG_H
#   include "config.h"
#endif // HAVE_CONFIG_H

// C++ includes, std and otherwise
#include <iomanip>
#include <iostream>
#include <string>
#include <vector>

using std::cout;
using std::endl;
using std::ostream;
using std::setw;
using std::string;
using std::vector;

// C++ includes
#include "AggregationUnion.H"
#include "Attribute.H"
#include "Bootstrap.H"
#include "Dimension.H"
#include "NetcdfDataset.H"
#include "Pack.H"
#include "Util.H"
#include "Variable.H"


ostream& operator << (ostream& os, const vector<int64_t> &vec)
{
    os << "(";
    vector<int64_t>::const_iterator iter;
    vector<int64_t>::const_iterator end = (vec.end() - 1);
    for (iter=vec.begin(); iter!=end; ++iter) {
        os << (*iter) << ",";
    }
    os << vec.back() << ")";
    return os;
}


int main(int argc, char **argv)
{
    int ndim = 3;
    int64_t *dims1 = new int64_t[ndim];
    int64_t *result1 = new int64_t[ndim];
    vector<int64_t> dims2;
    vector<int64_t> result2;
    dims1[0] = 2; dims2.push_back(2);
    dims1[1] = 3; dims2.push_back(3);
    dims1[2] = 4; dims2.push_back(4);

    pagoda::initialize(&argc, &argv);

    //cout << "dims=" << dims2 << endl;
    //printf("dims=[%lld,%lld,%lld]\n", dims1[0], dims1[1], dims1[2]);
    for (size_t i=0; i<26; ++i) {
        pagoda::unravel64(i, 3, dims1, result1);
        pagoda::unravel64(i, dims2, result2);
        cout << "unravel64(" << i << ", " << dims2 << ", " << result2 << ")";
        printf("\tunravel64(%zd, %d, [%ld,%ld,%ld], [%ld,%ld,%ld])\n",
                i, ndim,
                (long)dims1[0], (long)dims1[1], (long)dims1[2],
                (long)result1[0], (long)result1[1], (long)result1[2]);
    }

    delete [] dims1;
    delete [] result1;

    // Must always call to exit cleanly.
    pagoda::finalize();

    return EXIT_SUCCESS;
}
