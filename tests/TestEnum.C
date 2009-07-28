#include <iostream>
using namespace std;

#include <stdlib.h>

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
    int size_src = 10;
    int size_dst = 30;
    int g_src = NGA_Create(MT_INT, 1, &size_src, "src", NULL);
    int g_dst = NGA_Create(MT_INT, 1, &size_dst, "dst", NULL);
    int g_msk = NGA_Create(MT_INT, 1, &size_dst, "msk", NULL);
    int NEG_ONE = -1;

    if (0 == me) {
        int msk[] = {0,1,1,0,0,0,0,1,1,0,
                     1,0,0,1,0,1,0,1,0,0,
                     0,0,1,0,1,0,0,0,0,0};
        int ZERO = 0;
        int hi = size_dst-1;
        NGA_Put(g_msk, &ZERO, &hi, msk, NULL);
    }

    GA_Fill(g_dst, &NEG_ONE);
    enumerate(g_src, 2, 3);
    unpack1d(g_src, g_dst, g_msk);

    GA_Print(g_src);
    GA_Print(g_dst);
    GA_Print(g_msk);

    GA_Terminate();
    MPI_Finalize();
    exit(EXIT_SUCCESS);
};
