#include <stdlib.h>

#include <ga.h>
#include <macdecls.h>
#include <mpi.h>

int main(int argc, char **argv)
{
    MPI_Init(&argc, &argv);
    GA_Initialize();
    if (MA_init(MT_DBL, 1000, 1000) == MA_FALSE) {
        GA_Error("MA_init failed", 0);
    }

    int me = GA_Nodeid();
    int nproc = GA_Nnodes();
    int dims = 10;
    int g_src = NGA_Create(MT_INT, 1, &dims, "src", NULL);
    int g_dst0 = NGA_Create(MT_INT, 1, &dims, "Scan_add_excl0", NULL);
    int g_dst1 = NGA_Create(MT_INT, 1, &dims, "Scan_add_excl1", NULL);
    int g_dst2 = NGA_Create(MT_INT, 1, &dims, "Scan_copy", NULL);
    int g_mask = NGA_Create(MT_INT, 1, &dims, "mask", NULL);
    int ONE = 1;
    int ZERO = 0;
    int dim_1 = dims-1;

    GA_Zero(g_mask);
    GA_Sync();

    if (0 == me) {
        //int vals[] = {0,0,1,0,1,1,1,0,0,1}; // 10 == dims
        int vals[] = {1,0,1,0,1,1,1,0,0,1}; // 10 == dims
        int lo = 0;
        int hi = dims-1;

        NGA_Put(g_src, &lo, &hi, vals, NULL);
        NGA_Put(g_mask, &ZERO, &ZERO, &ONE, NULL);
    }
    GA_Sync();
    GA_Print(g_src);
    GA_Print(g_mask);
    GA_Scan_add(g_src, g_dst0, g_mask, ZERO, dim_1, ZERO);
    GA_Scan_add(g_src, g_dst1, g_mask, ZERO, dim_1, ONE);
    GA_Scan_copy(g_src, g_dst2, g_mask, ZERO, dim_1);
    GA_Sync();
    GA_Print(g_dst0);
    GA_Print(g_dst1);
    GA_Print(g_dst2);

    int dumb_dims[] = { 10, 20, 30, 40 };
    int dumb = NGA_Create(MT_INT, 4, dumb_dims, "dumb", NULL);
    if (0 == me) {
        int dlo[4];
        int dhi[4];
        int dld[3];
        int size = 1;
        for (int d=0; d<4; ++d) {
            size *= dhi[d]-dlo[d]+1;
        }
        int *dptr = new int[size];
        NGA_Distribution(dumb, me, dlo, dhi);
        NGA_Get(dumb, dlo, dhi, dptr, dld);
        printf("dumb lo=(%d,%d,%d,%d)\n", dlo[0], dlo[1], dlo[2], dlo[3]);
        printf("dumb hi=(%d,%d,%d,%d)\n", dhi[0], dhi[1], dhi[2], dhi[3]);
        printf("dumb ld=(%d,%d,%d)\n", dld[0], dld[1], dld[2]);
        delete [] dptr;
    }
    GA_Sync();

    GA_Terminate();
    MPI_Finalize();
    exit(EXIT_SUCCESS);
};
