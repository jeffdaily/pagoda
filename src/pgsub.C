#if HAVE_CONFIG_H
#   include <config.h>
#endif

#include <stdint.h>

#include <cstdlib>
#include <map>
#include <stdexcept>
#include <string>
#include <vector>

#include "Array.H"
#include "Bootstrap.H"
#include "CommandException.H"
#include "Dataset.H"
#include "Debug.H"
#include "Dimension.H"
#include "FileWriter.H"
#include "Grid.H"
#include "MaskMap.H"
#include "SubsetterCommands.H"
#include "Util.H"
#include "Variable.H"

using std::exception;
using std::string;
using std::map;
using std::vector;


#ifdef F77_DUMMY_MAIN
#   ifdef __cplusplus
extern "C"
#   endif
int F77_DUMMY_MAIN()
{
    return 1;
}
#endif

//#define READ_ALL

int main(int argc, char **argv)
{
    SubsetterCommands cmd;
    Dataset *dataset;
    vector<Variable*> vars;
    vector<Variable*>::iterator var_it;
    vector<Dimension*> dims;
    FileWriter *writer;
    vector<Grid*> grids;
    Grid *grid;
    MaskMap *masks;
    map<string,Array*> nb_arrays;
    map<string,Array*>::iterator nb_array_it;

    try {
        pagoda::initialize(&argc, &argv);

        cmd.parse(argc,argv);

        if (cmd.is_requesting_info()) {
            pagoda::print_zero(cmd.get_info());
            pagoda::finalize();
            return EXIT_SUCCESS;
        }

        dataset = cmd.get_dataset();
        vars = cmd.get_variables(dataset);
        dims = cmd.get_dimensions(dataset);

        grids = dataset->get_grids();
        if (grids.empty()) {
            pagoda::print_zero("no grid found\n");
            grid = NULL;
        }
        else {
            grid = grids[0];
        }

        masks = new MaskMap(dataset);
        masks->modify(cmd.get_index_hyperslabs());
        masks->modify(cmd.get_coord_hyperslabs(), grid);
        masks->modify(cmd.get_boxes(), grid);

        writer = cmd.get_output();
        writer->write_atts(cmd.get_attributes(dataset));
        writer->def_dims(dims);
        writer->def_vars(vars);

        // read/subset and write each variable...
#ifdef READ_ALL
        if (cmd.is_nonblocking()) {
            // read all variables, non-blocking
            for (var_it=vars.begin(); var_it!=vars.end(); ++var_it) {
                Variable *var = *var_it;
                nb_arrays[var->get_name()] = var->iread();
            }
            dataset->wait();
            // write all variables, non-blocking
            for (var_it=vars.begin(); var_it!=vars.end(); ++var_it) {
                Variable *var = *var_it;
                nb_array_it = nb_arrays.find(var->get_name());
                if (nb_array_it != nb_arrays.end()) {
                    writer->iwrite(nb_array_it->second,nb_array_it->first);
                }
            }
            writer->wait();
            // delete all the non-blocking arrays
            for (nb_array_it=nb_arrays.begin(); nb_array_it!=nb_arrays.end();
                    ++nb_array_it) {
                delete nb_array_it->second;
            }
            nb_arrays.clear();
        } else {
            for (var_it=vars.begin(); var_it!=vars.end(); ++var_it) {
                Variable *var = *var_it;
                Array *array = var->read();
                writer->write(array, var->get_name());
                delete array;
            }
        }
#else
        if (cmd.is_nonblocking()) {
            // read all non-record variables first, non-blocking
            for (var_it=vars.begin(); var_it!=vars.end(); ++var_it) {
                Variable *var = *var_it;
                if (!var->has_record()) {
                    nb_arrays[var->get_name()] = var->iread();
                }
            }
            dataset->wait();
            // write all non-record variables first, non-blocking
            for (var_it=vars.begin(); var_it!=vars.end(); ++var_it) {
                Variable *var = *var_it;
                nb_array_it = nb_arrays.find(var->get_name());
                if (nb_array_it != nb_arrays.end()) {
                    writer->iwrite(nb_array_it->second,nb_array_it->first);
                }
            }
            writer->wait();
            // delete all the non-blocking arrays
            for (nb_array_it=nb_arrays.begin(); nb_array_it!=nb_arrays.end();
                    ++nb_array_it) {
                delete nb_array_it->second;
            }
            nb_arrays.clear();
            // read/write all record variables record-at-a-time, blocking
            for (var_it=vars.begin(); var_it!=vars.end(); ++var_it) {
                Variable *var = *var_it;
                if (var->has_record()) {
                    Array *array = NULL; // reuse allocated array each record
                    for (int64_t rec=0,limit=var->get_nrec(); rec<limit; ++rec)
                    {
                        array = var->read(rec, array);
                        writer->write(array, var->get_name(), rec);
                    }
                    delete array;
                }
            }
        } else {
            for (var_it=vars.begin(); var_it!=vars.end(); ++var_it) {
                Variable *var = *var_it;
                if (var->has_record()) {
                    Array *array = NULL; // reuse allocated array each record
                    for (int64_t rec=0,limit=var->get_nrec(); rec<limit; ++rec)
                    {
                        array = var->read(rec, array);
                        writer->write(array, var->get_name(), rec);
                    }
                    delete array;
                }
                else {
                    Array *array = var->read();
                    ASSERT(NULL != array);
                    writer->write(array, var->get_name());
                    delete array;
                }
            }
        }
#endif

        // clean up
        delete dataset;
        delete masks;
        delete writer;

        pagoda::finalize();

    }
    catch (CommandException &ex) {
        pagoda::println_zero(ex.what());
        pagoda::finalize();
        return EXIT_FAILURE;
    }
    catch (exception &ex) {
        pagoda::abort(ex.what());
    }

    return EXIT_SUCCESS;
}
