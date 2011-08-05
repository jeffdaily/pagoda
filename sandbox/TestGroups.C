#ifdef HAVE_CONFIG_H
#   include "config.h"
#endif

#include <stdint.h>

// C++ includes, std and otherwise
#include <cstdio>
#include <cstdlib>
#include <iomanip>
#include <iostream>
#include <string>
#include <vector>

#include <mpi.h>
#include <ga.h>

#include "pagoda.H"

int main(int argc, char **argv)
{
    pagoda::initialize(&argc, &argv);

#if 0
    ProcessGroup group1(pagoda::me);
    pagoda::println_sync(pagoda::to_string(group1.get_id()) + " "
            + pagoda::vec_to_string(group1.get_ranks()) + " "
            + pagoda::to_string(ProcessGroup::get_default().get_id()));

    ProcessGroup group2(0);
    pagoda::println_sync(pagoda::to_string(group2.get_id()) + " "
            + pagoda::vec_to_string(group2.get_ranks()) + " "
            + pagoda::to_string(ProcessGroup::get_default().get_id()));
    
    ProcessGroup group3(pagoda::me%2);
    pagoda::println_sync(pagoda::to_string(group3.get_id()) + " "
            + pagoda::vec_to_string(group3.get_ranks()) + " "
            + pagoda::to_string(ProcessGroup::get_default().get_id()));

    ProcessGroup group4(pagoda::me>=(pagoda::npe/2));
    pagoda::println_sync(pagoda::to_string(group4.get_id()) + " "
            + pagoda::vec_to_string(group4.get_ranks()) + " "
            + pagoda::to_string(ProcessGroup::get_default().get_id()));
#endif
    const int NUM_GROUPS(2);
    vector<int64_t> shape;
    shape.push_back(3);
    shape.push_back(4);
    shape.push_back(5);

    // create array on world group
    Array *world_array = Array::create(DataType::FLOAT, shape);
    world_array->fill_value(0);

    // create group(s), automatically sets default to local group
    int color = pagoda::me/(pagoda::npe/NUM_GROUPS);
    ProcessGroup group(color);

    // create array on local group
    Array *group_array = Array::create(DataType::FLOAT, shape);
    group_array->fill_value(color+1);

    if (world_array->owns_data()) {
        float *buf = static_cast<float*>(world_array->access());
        vector<int64_t> lo,hi;
        world_array->get_distribution(lo,hi);
        for (int i=0; i<NUM_GROUPS; ++i) {
            if (i == color) {
                float *rec = static_cast<float*>(group_array->get(lo,hi));
                printf("first val of %d is %f\n", i, rec[0]);
            }
            pagoda::barrier();
        }
    }



    pagoda::finalize();
    return EXIT_SUCCESS;
}
