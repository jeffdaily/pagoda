#if HAVE_CONFIG_H
#   include "config.h"
#endif

#include <fstream>
#include <stdexcept>

#include "Bootstrap.H"
#include "Dataset.H"
#include "Error.H"
#include "PagodaException.H"
#include "Variable.H"

string get_filename(int argc, char **argv)
{
    if (argc > 1) {
        return argv[1];
    } else {
        if ((std::ifstream("./data/FillValue.nc"))) {
            return "./data/FillValue.nc";
        } else if ((std::ifstream("./FillValue.nc"))) {
            return "FillValue.nc";
        } else {
            ERR("cannot locate FillValue.nc");
        }
    }
}

int main(int argc, char **argv)
{
    Dataset *dataset;
    Variable *var;

    try {
        pagoda::initialize(&argc,&argv);

        dataset = Dataset::open(get_filename(argc,argv));
        ASSERT(NULL != dataset);
        var = dataset->get_var("first");
        ASSERT(NULL != var);
        ASSERT(var->has_fill_value());
        ASSERT(1.0 == var->get_fill_value());
        var = dataset->get_var("second");
        ASSERT(NULL != var);
        ASSERT(var->has_fill_value());
        ASSERT(2 == var->get_fill_value());
        var = dataset->get_var("third");
        ASSERT(NULL != var);
        ASSERT(false == var->has_fill_value());
        delete dataset;

        pagoda::finalize();
    } catch (PagodaException &ex) {
        pagoda::abort(ex.what());
    } catch (std::exception &ex) {
        pagoda::abort(ex.what());
    }

    return EXIT_SUCCESS;
}
