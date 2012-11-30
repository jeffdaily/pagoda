/**
 * Test the Numeric.H header.
 */
#include <algorithm>
#include <string>
#include <vector>

#include "Array.H"
#include "Bootstrap.H"
#include "Collectives.H"
#include "DataType.H"
#include "Numeric.H"
#include "Print.H"
#include "Util.H"

using std::generate;
using std::string;
using std::vector;

#define DEBUG 1
#if DEBUG
#   define STR_ARR(vec,n) pagoda::arr_to_string(vec,n,",",#vec)
#endif
// class generator:
typedef struct gen_enum {
  int current;
  gen_enum(int _current=0) {current=_current;}
  int operator()() {return current++;}
} gen_enum_t;

int main(int argc, char **argv)
{
    int *dst = NULL;
    vector<int64_t> dst_shape;
    int64_t dst_size;
    Array *dst_array;

    float *src = NULL;
    vector<int64_t> src_shape;
    vector<int64_t> src_brd_shape;
    int64_t src_size;
    Array *src_array;

    pagoda::initialize(&argc,&argv);

    dst_shape.push_back(2);
    dst_shape.push_back(3);
    dst_shape.push_back(4);
    dst_size = pagoda::shape_to_size(dst_shape);
    dst = new int[dst_size];
    generate(dst, dst+dst_size, gen_enum_t(1));

    src_shape.push_back(3);
    src_brd_shape.push_back(0); // for broadcast
    src_brd_shape.push_back(3);
    src_brd_shape.push_back(0); // for broadcast
    src_size = pagoda::shape_to_size(src_shape);
    src = new float[src_size];
    generate(src, src+src_size, gen_enum_t(2));

    pagoda::println_zero(STR_ARR(src, src_size));

    pagoda::in_place_broadcast_op(
            dst, dst_shape,
            src, src_brd_shape,
            pagoda::iadd<int,float>);

    pagoda::println_zero(STR_ARR(dst, dst_size));

    pagoda::in_place_broadcast_add(
            dst, dst_shape, DataType::INT,
            src, src_brd_shape, DataType::FLOAT);

    pagoda::println_zero(STR_ARR(dst, dst_size));

    dst_array = Array::create(DataType::INT, dst_shape);
    src_array = Array::create(DataType::FLOAT, src_shape);
    if (pagoda::me == 0) {
        dst_array->put(dst);
        src_array->put(src);
    }
    pagoda::barrier();
    dst_array->imul(src_array, src_brd_shape);
    dst_array->dump();

    pagoda::finalize();

    return EXIT_SUCCESS;
}
