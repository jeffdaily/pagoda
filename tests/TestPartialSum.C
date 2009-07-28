#include <cstdlib>
#include <iomanip>
    using std::setw;
#include <iostream>
    using std::cout;
    using std::endl;

#include <ga.h>
#include <macdecls.h>
#include <mpi.h>

#include "Pack.H"

int main(int argc, char **argv)
{
    MPI_Init(&argc, &argv);
    GA_Initialize();
    if (MA_init(MT_DBL, 1000, 1000) == MA_FALSE) {
        GA_Error("MA_init failed", 0);
    }

    int me = GA_Nodeid();
    int nproc = GA_Nnodes();
    int size = 10;
    int g_src = NGA_Create(MT_INT, 1, &size, "src", NULL);
    int g_dst0 = GA_Duplicate(g_src, "dst0");
    int g_dst1 = GA_Duplicate(g_src, "dst1");
    int ZERO = 0;
    int hi = size - 1;

    if (0 == me) {
        int vals[] = {0,0,1,0,1,1,1,0,0,1}; // 10 == size
        //int vals[] = {1,0,1,0,1,1,1,0,0,1}; // 10 == size
        int lo = 0;
        int hi = size-1;
        int ONE = 1;
        int ZERO = 0;
        NGA_Put(g_src, &lo, &hi, vals, NULL);
    }
    partial_sum(g_src, g_dst0, 0);
    GA_Sync();
    partial_sum(g_src, g_dst1, 1);
    GA_Sync();

    if (0 == me) {
        int *values = new int[size];
        int varw = 10;
        int valw = 3;

        cout << setw(varw) << "i = ";
        for (int i=0; i<size; ++i) {
            cout << setw(valw) << i;
        }
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

    GA_Terminate();
    MPI_Finalize();
    exit(EXIT_SUCCESS);
};
