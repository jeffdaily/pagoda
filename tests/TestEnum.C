/**
 * @file TestEnum.C
 *
 * Whether we got the pagoda::enumeration right.
 */
#if HAVE_CONFIG_H
#   include <config.h>
#endif

#include <cstdlib>
#include <iostream>

#include "Array.H"
#include "Bootstrap.H"
#include "gax.H"
#include "Pack.H"
#include "Util.H"

using std::cout;
using std::endl;


int main(int argc, char **argv)
{
    int me = Util::nodeid();
    int nproc = Util::num_nodes();
    vector<int64_t> shape_src(1,10);
    vector<int64_t> shape_dst(1,30);
    Array *a_src = NULL;
    Array *a_enm = NULL;
    Array *a_dst = NULL;
    Array *a_msk = NULL;
    int NEG_ONE = -1;
    int TWO = 2;
    int THREE = 3;
    double START = 3.15;
    double STEP = 0.05;
    vector<int64_t> lo;
    vector<int64_t> hi;

    pagoda::initialize(&argc, &argv);
    Util::calculate_required_memory();

    a_src = Array::create(DataType::INT,    shape_src);
    a_enm = Array::create(DataType::DOUBLE, shape_dst);
    a_dst = Array::create(DataType::INT,    shape_dst);
    a_msk = Array::create(DataType::INT,    shape_dst);

    if (0 == me) {
        int msk[] = {0,1,1,0,0,0,0,1,1,0,
                     1,0,0,1,0,1,0,1,0,0,
                     0,0,1,0,1,0,0,0,0,0};
        vector<int64_t> ZERO(1,0);
        vector<int64_t> hi(1,shape_dst[0]-1);

        a_msk->put(msk, ZERO, hi);
    }

    a_dst->fill(&NEG_ONE);
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
