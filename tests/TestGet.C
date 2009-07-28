#include <ga.h>
#include <macdecls.h>
#include <mpi.h>

#include <cstdlib>
#include <iomanip>
#include <iostream>
using namespace std;


int main(int argc, char **argv)
{
    MPI_Init(&argc, &argv);
    GA_Initialize();
    if (MA_init(MT_DBL, 1000, 1000) == MA_FALSE) {
        GA_Error("MA_init failed", 0);
    }

    int nproc = GA_Nnodes();
    int me = GA_Nodeid();

    int data_dim1 = 15;
    int data_dim2 = 10;
    int data_dims[] = {data_dim1,data_dim2};
    int data_size = data_dim1*data_dim2;
    int *data;
    int data_lo[2];
    int data_hi[2];
    int data_ld[1];
    int g_a = NGA_Create(MT_INT, 2, data_dims, "data", NULL);

    int buf_dim1 = 5;
    int buf_dim2 = 10;
    int buf_size = buf_dim1*buf_dim2;
    int buf[buf_size];

    int lo[] = {10,0};
    int hi[] = {14,4};
    //int ld[] = {10};
    int ld[] = {10};

    cout << "data" << endl;
    NGA_Distribution(g_a, me, data_lo, data_hi);
    NGA_Access(g_a, data_lo, data_hi, &data, data_ld);
    for (int i=0,ilimit=data_hi[0]-data_lo[0]+1; i<ilimit; ++i) {
        for (int j=0,jlimit=data_hi[1]-data_lo[1]+1; j<jlimit; ++j) {
            data[i*jlimit + j] = i*jlimit+j;
            cout << setw(4) << i*jlimit+j;
        }
        cout << endl;
    }
    NGA_Release_update(g_a, data_lo, data_hi);

    for (int i=0; i<buf_size; ++i) {
        buf[i] = i+1000;
    }

    cout << "buffer before get" << endl;
    for (int i=0; i<buf_dim1; ++i) {
        for (int j=0; j<buf_dim2; ++j) {
            cout << setw(5) << buf[i*buf_dim2 + j];
        }
        cout << endl;
    }

    NGA_Get(g_a, lo, hi, buf, ld);

    cout << "buffer after get" << endl;
    for (int i=0; i<buf_dim1; ++i) {
        for (int j=0; j<buf_dim2; ++j) {
            cout << setw(5) << buf[i*buf_dim2 + j];
        }
        cout << endl;
    }

    int ddims[] = {2,3,4,5,6};
    int dld[5];
    int dlo[5];
    int dhi[5];
    int *ddata;
    int a = NGA_Create(MT_INT, 2, ddims, "a", NULL);
    int b = NGA_Create(MT_INT, 3, ddims, "b", NULL);
    int c = NGA_Create(MT_INT, 4, ddims, "c", NULL);
    int d = NGA_Create(MT_INT, 5, ddims, "d", NULL);
    NGA_Distribution(a, me, dlo, dhi);
    NGA_Access(a, dlo, dhi, &ddata, dld);
    for (int i=0; i<1; ++i) cout << dld[i] << " "; cout << endl;
    NGA_Distribution(b, me, dlo, dhi);
    NGA_Access(b, dlo, dhi, &ddata, dld);
    for (int i=0; i<2; ++i) cout << dld[i] << " "; cout << endl;
    NGA_Distribution(c, me, dlo, dhi);
    NGA_Access(c, dlo, dhi, &ddata, dld);
    for (int i=0; i<3; ++i) cout << dld[i] << " "; cout << endl;
    NGA_Distribution(d, me, dlo, dhi);
    NGA_Access(d, dlo, dhi, &ddata, dld);
    for (int i=0; i<4; ++i) cout << dld[i] << " "; cout << endl;
    GA_Terminate();
    MPI_Finalize();
    exit(EXIT_SUCCESS);
};
