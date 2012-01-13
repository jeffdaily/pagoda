/**
 * Test the partial_sum operation on a handful of different, small inputs.
 * Compute the same operation locally on each process and compare to the global
 * result.
 */
#if HAVE_CONFIG_H
#   include "config.h"
#endif

#include <stdint.h>

#include <cstdlib>
#include <iomanip>
#include <iostream>
#include <vector>

using std::cout;
using std::endl;
using std::setw;
using std::vector;

#include "Array.H"
#include "Bootstrap.H"
#include "DataType.H"
#include "Pack.H"
#include "Print.H"
#include "Util.H"

static void partial_sum(int *src, int *dst, int size, bool excl)
{
    int i;

    for (i=0; i<size; ++i) {
        if (0 != src[i]) {
            break;
        }
    }
    if (i == size) {
        return;
    }

    if (excl) {
        dst[i] = src[i];
        ++i;
        for (; i<size; ++i) {
            dst[i] = dst[i-1] + src[i];
        }
    }
    else {
        dst[i] = 0;
        ++i;
        for (; i<size; ++i) {
            dst[i] = dst[i-1] + src[i-1];
        }
    }
}


static int cmp(int *left, int *right, int size)
{
    for (int i=0; i<size; ++i) {
        if (left[i] != right[i]) {
            return 1;
        }
    }
    return 0;
}


int main(int argc, char **argv)
{
    Array *g_src;
    Array *g_dst0;
    Array *g_dst1;
    vector<int64_t> size(1,10);
    vector<int64_t> ZERO(1,0);
    vector<int64_t> hi(1,size[0]-1);
    int vals0[] = {0,0,0,0,0,0,0,0,0,0};
    int vals1[] = {0,0,1,0,1,1,1,0,0,1};
    int vals2[] = {1,0,1,0,1,1,1,0,0,0};
    int vals3[] = {1,1,1,1,1,1,1,1,1,1};
    int *vals[] = {vals0, vals1, vals2, vals3};
    int retcode = 0;

    pagoda::initialize(&argc, &argv);

    g_src = Array::create(DataType::INT, size);
    g_dst0 = Array::create(DataType::INT, size);
    g_dst1 = Array::create(DataType::INT, size);

    for (int loop=0; loop<4; ++loop) {
        vector<int> global_result(10,0);
        vector<int> local_result(10,0);

        /* global operation */
        if (0 == pagoda::me) {
            g_src->put(vals[loop], ZERO, hi);
        }
        pagoda::barrier();
        pagoda::partial_sum(g_src, g_dst0, false);
        pagoda::barrier();
        pagoda::partial_sum(g_src, g_dst1, true);
        pagoda::barrier();

        /* local operation and check */
        partial_sum(vals[loop], &local_result[0], 10, false);
        (void)g_dst0->get(ZERO, hi, &global_result[0]);
        retcode = cmp(&local_result[0], &global_result[0], 10);
        pagoda::gop_sum(retcode);
        if (0 != retcode) {
            pagoda::println_zero("excl=false loop=" + pagoda::to_string(loop));
            pagoda::println_sync(pagoda::arr_to_string(vals[loop], 10,
                        ",", "         vals"));
            pagoda::println_sync(pagoda::vec_to_string(global_result,
                        ",", "global_result"));
            pagoda::println_sync(pagoda::vec_to_string(local_result,
                        ",", " local_result"));
            pagoda::finalize();
            return retcode;
        }

        /* reset buffers */
        global_result.assign(10,0);
        local_result.assign(10,0);

        /* local operation and check */
        partial_sum(vals[loop], &local_result[0], 10, true);
        (void)g_dst1->get(ZERO, hi, &global_result[0]);
        retcode = cmp(&local_result[0], &global_result[0], 10);
        pagoda::gop_sum(retcode);
        if (0 != retcode) {
            pagoda::println_zero("excl=true");
            pagoda::println_sync(pagoda::arr_to_string(vals[loop], 10,
                        ",", "         vals"));
            pagoda::println_sync(pagoda::vec_to_string(global_result,
                        ",", "global_result"));
            pagoda::println_sync(pagoda::vec_to_string(local_result,
                        ",", " local_result"));
            pagoda::finalize();
            return retcode;
        }
    }

    pagoda::finalize();
    return EXIT_SUCCESS;
};
