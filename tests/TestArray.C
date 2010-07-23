#if HAVE_CONFIG_H
#   include "config.h"
#endif

#include "Array.H"
#include "Bootstrap.H"
#include "DataType.H"


int main(int argc, char **argv)
{
    Array *array1;
    Array *array2;
    Array *array3;
    vector<int64_t> shape1;
    vector<int64_t> shape2;
    int ONE = 1;
    int TWO = 2;

    pagoda::initialize(&argc,&argv);

    shape1.push_back(10);
    shape1.push_back(20);

    shape2.push_back(20);

    array1 = Array::create(DataType::INT, shape1);
    array2 = Array::create(DataType::INT, shape2);
    array1->fill(&ONE);
    array2->fill(&TWO);

    array3 = array1->add(array1);
    array3->dump();

    array3 = array1->add(array2);
    array3->dump();

    pagoda::finalize();

    return EXIT_SUCCESS;
}
