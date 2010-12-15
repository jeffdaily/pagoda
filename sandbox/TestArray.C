#if HAVE_CONFIG_H
#   include "config.h"
#endif

#include <cstdlib>
#include <iostream>

#include "Array.H"
#include "Bootstrap.H"
#include "DataType.H"
#include "Debug.H"
#include "Util.H"

using std::cout;
using std::endl;


template <class T>
void check(DataType type)
{
    Array *g_a;
    int64_t n = 256;
    //int64_t m = n*2;
    //int64_t inc;
    //int64_t ij;
    T *a;
    T *b;
    //T v[m];
    //T w[m];
    vector<int64_t> lo(2,0);
    vector<int64_t> hi(2,n-1);
    vector<int64_t> tlo(2,0);
    vector<int64_t> thi(2,n-1);
    vector<int64_t> tld(1,0);

    a = new T[n*n];
    b = new T[n*n];

    // a is local copy of what the Array should start as
    for (int i=0; i<n; i++) {
        for (int j=0; j<n; j++) {
            a[i*n + j] = i*n + j;
            b[i*n + j] = -1;
        }
    }
    g_a = Array::create(type, vector<int64_t>(2,n));

    // zero the array
    g_a->fill_value(0);
    
    // check that it is indeed zero
    b = (T*)g_a->get(lo, hi, b);

    for (int i=0; i<n; i++) {
        for (int j=0; j<n; j++) {
            if (b[i*n + j] != 0) {
                cout << " zero " << pagoda::me << " " << i << " " << j
                    << " " << b[i*n + j] << endl;
            }
        }
    }
    pagoda::print_zero("\n");
    pagoda::print_zero("Array::fill zero is OK\n");
    pagoda::print_zero("\n");

    /* TODO finish porting GA's test.F
     *
    // each node fills in disjoint sections of the array
    pagoda::print_zero("> Checking disjoint put ... \n");
    inc = n/20;
    ij = 0;
    tld[0] = n;
    for (int i=0; i<n; i+=inc) {
        for (int j=0; j<n; j+=inc) {
            if (ij % pagoda::npe == pagoda::me) {
                tlo[0] = i;
                thi[0] = i+inc < n-1 ? i+inc : n-1;
                tlo[1] = j;
                thi[1] = j+inc < n-1 ? j+inc : n-1;
                g_a->put(&a[tlo[0]*n+thi[0]], tlo, thi, tld);
            }
            ij++;
        }
    }
    // all nodes check all of g_a
    b = (T*)g_a->get(b, lo, hi);
    for (int i=0; i<n; i++) {
        for (int j=0; j<n; j++) {
            if (b[i*n + j] != a[i*n + j]) {
                cout << " put " << pagoda::me << " " << i << " " << j
                    << " " << a[i*n + j] << " " << b[i*n + j] << endl;
                pagoda::abort("... exiting");
            }
        }
    }
    */
    
    delete [] a;
    delete [] b;
    delete g_a;
}


int main(int argc, char **argv)
{
    Array *array1;
    Array *array2;
    Array *array3;
    vector<int64_t> shape1;
    vector<int64_t> shape2;

    pagoda::initialize(&argc,&argv);

    shape1.push_back(10);
    shape1.push_back(20);

    shape2.push_back(20);

    array1 = Array::create(DataType::INT, shape1);
    array2 = Array::create(DataType::INT, shape2);
    array1->fill_value(1);
    array2->fill_value(2);

    array3 = array1->add(array1);
    array3->dump();

    array3 = array1->add(array2);
    array3->dump();

    check<double>(DataType::DOUBLE);

    pagoda::finalize();

    return EXIT_SUCCESS;
}
