/**
 * @file TestEnum.C
 *
 * Whether we got the pagoda::enumeration right.
 */
#if HAVE_CONFIG_H
#   include <config.h>
#endif

#include <stdint.h>

#include <cstdlib>
#include <iostream>
#include <vector>

using std::cout;
using std::endl;
using std::vector;

#include "Array.H"
#include "Bootstrap.H"
#include "gax.H"
#include "Pack.H"
#include "Util.H"


int main(int argc, char **argv)
{
    vector<int64_t> shape_src(1,10);
    vector<int64_t> shape_dst(1,30);
    Array *a_src = NULL;
    Array *a_enm = NULL;
    Array *a_dst = NULL;
    Array *a_msk = NULL;
    int TWO = 2;
    int THREE = 3;
    double START = 3.15;
    double STEP = 0.05;
    vector<int64_t> lo;
    vector<int64_t> hi;

    pagoda::initialize(&argc, &argv);

    a_src = Array::create(DataType::INT,    shape_src);
    a_enm = Array::create(DataType::DOUBLE, shape_dst);
    a_dst = Array::create(DataType::INT,    shape_dst);
    a_msk = Array::create(DataType::INT,    shape_dst);

    if (0 == pagoda::me) {
        int msk[] = {0,1,1,0,0,0,0,1,1,0,
                     1,0,0,1,0,1,0,1,0,0,
                     0,0,1,0,1,0,0,0,0,0};
        vector<int64_t> ZERO(1,0);
        vector<int64_t> hi(1,shape_dst[0]-1);

        a_msk->put(msk, ZERO, hi);
    }

    a_dst->fill_value(-1);
    pagoda::enumerate(a_src, &TWO, &THREE);
    pagoda::enumerate(a_enm, &START, &STEP);
    pagoda::unpack1d(a_src, a_dst, a_msk);

    a_enm->dump();
    a_src->dump();
    a_msk->dump();
    a_dst->dump();

    pagoda::finalize();

    return EXIT_SUCCESS;
};
