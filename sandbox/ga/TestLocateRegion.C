#include <cstdlib>
#include <iostream>
    using std::cout;
    using std::endl;

#include <ga.h>
#include <macdecls.h>
#include <mpi.h>

void test1();
void test2();

int main(int argc, char **argv)
{
    MPI_Init(&argc, &argv);
    GA_Initialize();
    if (MA_init(MT_DBL, 1000, 1000) == MA_FALSE) {
        GA_Error("MA_init failed", 0);
    }

    test1();
    test2();

    GA_Terminate();
    MPI_Finalize();
    exit(EXIT_SUCCESS);
};


void test1()
{
    int me = GA_Nodeid();
    int nproc = GA_Nnodes();
    int ndim = 2;
    int dims[] = {100,200};
    int g_a = NGA_Create(MT_INT, ndim, dims, "test", NULL);
    int lo[] = {0,0};
    int hi[] = {99,199};
    int *map = new int[2*ndim*nproc];
    int *procs = new int[nproc];

    int result = NGA_Locate_region(g_a, lo, hi, map, procs);

    if (0 == me) {
        cout << "#procs with data=" << result << endl;
        for (int i=0; i<result; ++i) {
            cout << "[" << procs[i] << "]=";
            for (int j=0; j<(2*ndim); ++j) {
                cout << map[i*2*ndim+j] << ",";
            }
            cout << endl;
        }
    }

    delete [] map;
    delete [] procs;
}


void test2()
{
    int me = GA_Nodeid();
    int nproc = GA_Nnodes();
    int ndim = 1;
    int dims[] = {10};
    int g_a = NGA_Create(MT_INT, ndim, dims, "test", NULL);
    int lo[] = {0};
    int hi[] = {9};
    int *map = new int[2*ndim*nproc];
    int *procs = new int[nproc];

    int result = NGA_Locate_region(g_a, lo, hi, map, procs);

    int sum = 0;
    int *ptr = map;
    for (int i=0; i<result; ++i) {
        if (procs[i]<me) {
            sum += ptr[1]-ptr[0]+1;
            ptr += 2;
        } else if (procs[i]==me) {
            cout << '[' << me << "]=" << sum << endl;
        } else { // procs[i]>me
        }
        GA_Sync();
    }

    delete [] map;
    delete [] procs;
}

