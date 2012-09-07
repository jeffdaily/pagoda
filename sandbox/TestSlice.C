/**
 * @file TestMaskMap
 */
#if HAVE_CONFIG_H
#   include <config.h>
#endif

#include <cstdlib>
#include <iomanip>
#include <iostream>
using namespace std;

#include "Array.H"
#include "Bootstrap.H"
#include "DataType.H"
#include "Dimension.H"
#include "Debug.H"
#include "MaskMap.H"
#include "Slice.H"
#include "Util.H"


int main(int argc, char **argv)
{
    DimSlice slice0("name,10");
    DimSlice slice1("name", 5, 7, 1);
    DimSlice slice2("name,2,4");
    DimSlice slice3("name,9");
    int64_t start, stop, step;

    pagoda::initialize(&argc, &argv);

    if (0 == pagoda::nodeid()) {
        for (int size=11; size<=15; size++) {
            slice0.indices(size, start, stop, step);
            cout << slice0
                << " size=" << size
                << " start=" << start
                << " stop=" << stop
                << " step=" << step
                << endl;
        }
        for (int size=7; size<=10; size++) {
            slice1.indices(size, start, stop, step);
            cout << slice1
                << " size=" << size
                << " start=" << start
                << " stop=" << stop
                << " step=" << step
                << endl;
        }
    }

    pagoda::finalize();

    return EXIT_SUCCESS;
};
