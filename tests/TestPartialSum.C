#if HAVE_CONFIG_H
#   include "config.h"
#endif

#include <cstdlib>
#include <iomanip>
#include <iostream>

using std::setw;
using std::cout;
using std::endl;

#include "Array.H"
#include "Bootstrap.H"
#include "DataType.H"
#include "Pack.H"
#include "Util.H"

int main(int argc, char **argv)
{
    Array *g_src;
    Array *g_dst0;
    Array *g_dst1;
    vector<int64_t> size(1,10);
    vector<int64_t> ZERO(1,0);
    vector<int64_t> hi(1,size[0]-1);

    pagoda::initialize(&argc, &argv);
    Util::calculate_required_memory();

    g_src = Array::create(DataType::INT, size);
    g_dst0 = Array::create(DataType::INT, size);
    g_dst1 = Array::create(DataType::INT, size);

    for (int loop=0; loop<3; ++loop) {
        if (0 == pagoda::me) {
            if (0 == loop) {
                int vals[] = {0,0,1,0,1,1,1,0,0,1}; // 10 == size
                g_src->put(vals, ZERO, hi);
            } else if (1 == loop) {
                int vals[] = {1,0,1,0,1,1,1,0,0,0}; // 10 == size
                g_src->put(vals, ZERO, hi);
            } else if (2 == loop) {
                int vals[] = {1,1,1,1,1,1,1,1,1,1}; // 10 == size
                g_src->put(vals, ZERO, hi);
            }
        }
        pagoda::partial_sum(g_src, g_dst0, false);
        Util::barrier();
        pagoda::partial_sum(g_src, g_dst1, true);
        Util::barrier();

        if (0 == pagoda::me) {
            int *values = new int[size[0]];
            int varw = 10;
            int valw = 3;

            cout << endl;

            g_src->get(values, ZERO, hi, vector<int64_t>());
            cout << setw(varw) << "src[i] =";
            for (int i=0; i<size[0]; ++i) {
                cout << setw(valw) << values[i];
            }
            cout << endl;

            g_dst0->get(values, ZERO, hi, vector<int64_t>());
            cout << setw(varw) << "dst0[i] =";
            for (int i=0; i<size[0]; ++i) {
                cout << setw(valw) << values[i];
            }
            cout << endl;

            g_dst1->get(values, ZERO, hi, vector<int64_t>());
            cout << setw(varw) << "dst1[i] =";
            for (int i=0; i<size[0]; ++i) {
                cout << setw(valw) << values[i];
            }
            cout << endl;

            delete [] values;
        }
    }

    pagoda::finalize();
    return EXIT_SUCCESS;
};
