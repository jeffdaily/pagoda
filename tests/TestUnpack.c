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
    int size_dst = 10;
    int size_src = 6; // MUST MATCH proc 0 put of mask below
    //int g_src = NGA_Create(MT_INT, 1, &size_src, "src", NULL);
    int g_src = NGA_Create(MT_INT, 1, &size_dst, "src", NULL);
    int g_dst = NGA_Create(MT_INT, 1, &size_dst, "dst", NULL);
    int g_msk = NGA_Create(MT_INT, 1, &size_dst, "msk", NULL);
    int ONE_NEG = -1;
    int icount;

    GA_Fill(g_src, &ONE_NEG);
    GA_Fill(g_dst, &ONE_NEG);
    GA_Patch_enum(g_src, 0, size_src-1, 0, 1);
    if (0 == me) {
        // MUST MATCH size_src count above
        //int vals[] = {0,0,1,0,1,1,1,0,0,1}; // 10 == size_dst, count = 5
        int vals[] = {1,0,1,0,1,1,1,0,0,1}; // 10 == size_dst, count = 6
        int lo = 0;
        int hi = size_dst-1;

        NGA_Put(g_msk, &lo, &hi, vals, NULL);
    }
    GA_Sync();

    GA_Print(g_src);
    GA_Print(g_msk);
    GA_Print(g_dst);

    GA_Unpack(g_src, g_dst, g_msk, 0, size_dst-1, &icount);

    GA_Print(g_dst);

    GA_Terminate();
    MPI_Finalize();
    exit(EXIT_SUCCESS);
};
