/** Perform various block-sized pnetcdf reads. */
#ifdef HAVE_CONFIG_H
#   include <config.h>
#endif

#include <stdint.h>

#include <cstdlib>
#include <iostream>
#include <vector>

using std::cerr;
using std::cout;
using std::endl;
using std::vector;

#include "Array.H"
#include "Collectives.H"
#include "Bootstrap.H"
#include "Dataset.H"
#include "Debug.H"
#include "FileWriter.H"
#include "Util.H"
#include "Variable.H"


/**
 * 1) Open a dataset for writing.
 * 2) Write dummy values.
 */
int main(int argc, char **argv)
{
    Array *array1;
    Array *array2;
    Array *array3;
    vector<int64_t> shape1;
    vector<int64_t> shape2;
    vector<int64_t> shape3;
    vector<string> dims;
    FileWriter *writer;
    vector<int64_t> start;

    pagoda::initialize(&argc, &argv);

    shape1.push_back(10);
    shape1.push_back(20);
    shape2.push_back(20);
    shape3.push_back(5);
    shape3.push_back(10);

    array1 = Array::create(DataType::FLOAT, shape1);
    array2 = Array::create(DataType::FLOAT, shape2);
    array3 = Array::create(DataType::FLOAT, shape3);
    start.push_back(3);
    start.push_back(3);

    array1->fill_value(1);
    array2->fill_value(2);
    array3->fill_value(3);
    pagoda::barrier();

    writer = FileWriter::open("test.nc", FF_CDF1)->create();
    writer->def_dim("dim1", 10);
    writer->def_dim("dim2", 20);
    dims.push_back("dim1");
    dims.push_back("dim2");
    writer->def_var("var1", dims, DataType::FLOAT);
    writer->write(array1, "var1");
    writer->write(array2, "var1", 2);
    writer->write(array3, "var1", start);

    delete array1;
    delete array2;
    delete array3;
    delete writer;

    pagoda::finalize();

    return EXIT_SUCCESS;
}
