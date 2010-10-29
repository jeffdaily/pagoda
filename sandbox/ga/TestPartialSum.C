#include <cstdlib>
#include <iomanip>
#include <iostream>

using std::setw;
using std::cout;
using std::endl;

#include <ga.h>
#include <macdecls.h>
#include <mpi.h>

#include "gax.H"

int main(int argc, char **argv)
{
    int me;
    int g_src;
    int g_dst0;
    int g_dst1;
    int size = 10;
    int ZERO = 0;
    int hi = size - 1;

    MPI_Init(&argc, &argv);
    GA_Initialize();

    if (MA_init(MT_DBL, 1000, 1000) == MA_FALSE) {
        GA_Error("MA_init failed", 0);
    }

    me = GA_Nodeid();
    g_src = NGA_Create(MT_INT, 1, &size, "src", NULL);
    g_dst0 = GA_Duplicate(g_src, "dst0");
    g_dst1 = GA_Duplicate(g_src, "dst1");

    for (int loop=0; loop<3; ++loop) {
        if (0 == me) {
            if (0 == loop) {
                int vals[] = {0,0,1,0,1,1,1,0,0,1}; // 10 == size
                NGA_Put(g_src, &ZERO, &hi, vals, NULL);
            } else if (1 == loop) {
                int vals[] = {1,0,1,0,1,1,1,0,0,0}; // 10 == size
                NGA_Put(g_src, &ZERO, &hi, vals, NULL);
            } else if (2 == loop) {
                int vals[] = {1,1,1,1,1,1,1,1,1,1}; // 10 == size
                NGA_Put(g_src, &ZERO, &hi, vals, NULL);
            }
        }
        gax::partial_sum(g_src, g_dst0, 0);
        GA_Sync();
        gax::partial_sum(g_src, g_dst1, 1);
        GA_Sync();

        if (0 == me) {
            int *values = new int[size];
            int varw = 10;
            int valw = 3;

            cout << endl;

            NGA_Get(g_src, &ZERO, &hi, values, NULL);
            cout << setw(varw) << "src[i] =";
            for (int i=0; i<size; ++i) {
                cout << setw(valw) << values[i];
            }
            cout << endl;

            NGA_Get(g_dst0, &ZERO, &hi, values, NULL);
            cout << setw(varw) << "dst0[i] =";
            for (int i=0; i<size; ++i) {
                cout << setw(valw) << values[i];
            }
            cout << endl;

            NGA_Get(g_dst1, &ZERO, &hi, values, NULL);
            cout << setw(varw) << "dst1[i] =";
            for (int i=0; i<size; ++i) {
                cout << setw(valw) << values[i];
            }
            cout << endl;

            delete [] values;
        }
    }

    GA_Terminate();
    MPI_Finalize();
    return EXIT_SUCCESS;
};
