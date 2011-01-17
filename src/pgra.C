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
#include "CommandLineOption.H"
#include "Dataset.H"
#include "Debug.H"
#include "Dimension.H"
#include "FileWriter.H"
#include "Grid.H"
#include "MaskMap.H"
#include "PgraCommands.H"
#include "ScalarArray.H"
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


void init_tally(Array *result, Array *tally, double fill);


int main(int argc, char **argv)
{
    PgraCommands cmd;
    Dataset *dataset = NULL;
    vector<Variable*> vars;
    vector<Variable*> record_vars;
    vector<Variable*> nonrecord_vars;
    vector<Variable*>::iterator var_it;
    vector<Dimension*> dims;
    FileWriter *writer = NULL;
    vector<Grid*> grids;
    Grid *grid = NULL;
    MaskMap *masks = NULL;
    string op;
    vector<Array*> nb_arrays;
    vector<Array*> nb_results;
    vector<Array*> nb_tallys;

    try {
        pagoda::initialize(&argc, &argv);

        cmd.parse(argc,argv);

        if (cmd.is_helping()) {
            pagoda::print_zero(cmd.get_usage());
            pagoda::finalize();
            return EXIT_SUCCESS;
        }

        dataset = cmd.get_dataset();
        vars = cmd.get_variables(dataset);
        dims = cmd.get_dimensions(dataset);
        op = cmd.get_operator();

        grids = dataset->get_grids();
        if (grids.empty()) {
            pagoda::print_zero("no grid found\n");
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

        if (cmd.is_nonblocking()) {
            Dimension *udim = dataset->get_udim();
            int64_t nrec = NULL != udim ? udim->get_size() : 0;

            // read all non-record variables first, cache record vars
            for (var_it=vars.begin(); var_it!=vars.end(); ++var_it) {
                Variable *var = *var_it;
                if (var->has_record()) {
                    record_vars.push_back(var);
                } else {
                    nonrecord_vars.push_back(var);
                    nb_arrays.push_back(var->iread());
                }
            }
            dataset->wait();
            // write all non-record variables first
            for (size_t i=0; i<nonrecord_vars.size(); ++i) {
                writer->iwrite(nb_arrays[i],nonrecord_vars[i]->get_name());
            }
            writer->wait();
            // delete all the non-blocking arrays
            for (size_t i=0; i<nb_arrays.size(); ++i) {
                delete nb_arrays[i];
            }
            nb_arrays.clear(); // probably not needed since assign later
            // read/write all record variables, record-at-a-time
            // prefill/size nb_XXX vectors
            nb_arrays.assign(record_vars.size(), NULL);
            nb_tallys.assign(record_vars.size(), NULL);
            nb_results.assign(record_vars.size(), NULL);
            // first record is optimized
            for (size_t i=0; i<record_vars.size(); ++i) {
                Variable *var = record_vars[i];
                nb_results[i] = var->iread(int64_t(0));
            }
            dataset->wait();
            for (size_t i=0; i<record_vars.size(); ++i) {
                Variable *var = record_vars[i];
                if (nb_results[i]->get_type() == DataType::CHAR) {
                    continue;
                }
                if (var->has_fill_value(0)) {
                    nb_tallys[i] = Array::create(
                                DataType::INT, nb_results[i]->get_shape());
                    init_tally(nb_results[i], nb_tallys[i],
                            var->get_fill_value(0));
                }
                if (op == OP_RMS || op == OP_RMSSDN || op == OP_AVGSQR) {
                    // square the values
                    nb_results[i]->imul(nb_results[i]);
                }
            }
            // remaining records
            for (int64_t r=1; r<nrec; ++r ) {
                for (size_t i=0; i<record_vars.size(); ++i) {
                    Variable *var = record_vars[i];
                    nb_arrays[i] = var->iread(r, nb_arrays[i]);
                }
                dataset->wait();
                for (size_t i=0; i<record_vars.size(); ++i) {
                    Variable *var = record_vars[i];
                    if (nb_results[i]->get_type() == DataType::CHAR) {
                        continue;
                    }
                    if (op == OP_MAX) {
                        nb_results[i]->imax(nb_arrays[i]);
                    }
                    else if (op == OP_MIN) {
                        nb_results[i]->imin(nb_arrays[i]);
                    }
                    else {
                        if (op == OP_RMS
                                || op == OP_RMSSDN
                                || op == OP_AVGSQR) {
                            // square the values
                            nb_arrays[i]->imul(nb_arrays[i]);
                        }
                        // sum the values or squares, keep a tally
                        nb_results[i]->set_counter(nb_tallys[i]);
                        nb_results[i]->iadd(nb_arrays[i]);
                        nb_results[i]->set_counter(NULL);
                    }
                }
            }
            // we can delete the temporary arrays now
            for (size_t i=0; i<record_vars.size(); ++i) {
                delete nb_arrays[i];
                nb_arrays[i] = NULL;
            }
            for (size_t i=0; i<record_vars.size(); ++i) {
                Variable *var = record_vars[i];
                // normalize, multiply, etc where necessary
                if (op == OP_AVG || op == OP_SQRT || op == OP_SQRAVG
                        || op == OP_RMS || op == OP_RMS || op == OP_AVGSQR) {
                    if (NULL != nb_tallys[i]) {
                        nb_results[i]->idiv(nb_tallys[i]);
                    } else {
                        ScalarArray scalar(nb_results[i]->get_type());
                        scalar.fill_value(nrec);
                        nb_results[i]->idiv(&scalar);
                    }
                }
                else if (op == OP_RMSSDN) {
                    if (NULL != nb_tallys[i]) {
                        ScalarArray scalar(nb_tallys[i]->get_type());
                        scalar.fill_value(1);
                        nb_tallys[i]->isub(&scalar);
                        nb_results[i]->idiv(nb_tallys[i]);
                    } else {
                        ScalarArray scalar(nb_results[i]->get_type());
                        scalar.fill_value(nrec-1);
                        nb_results[i]->idiv(&scalar);
                    }
                }

                // some operations require additional processing */
                if (op == OP_RMS || op == OP_RMSSDN || op == OP_SQRT) {
                    nb_results[i]->ipow(0.5);
                }
                else if (op == OP_SQRAVG) {
                    nb_results[i]->imul(nb_results[i]);
                }

                writer->iwrite(nb_results[i], var->get_name(), 0);
            }
            writer->wait();
            for (size_t i=0; i<record_vars.size(); ++i) {
                delete nb_results[i];
                nb_results[i] = NULL;
                if (NULL != nb_tallys[i]) {
                    delete nb_tallys[i];
                    nb_tallys[i] = NULL;
                }
            }
        } else {
            // read each variable in order
            // for record variables, read one record at a time
            for (var_it=vars.begin(); var_it!=vars.end(); ++var_it) {
                Variable *var = *var_it;
                if (var->has_record() && var->get_nrec() > 0) {
                    int64_t nrec = var->get_nrec();
                    Array *result = NULL;
                    Array *array = NULL; // reuse allocated array each record
                    Array *tally = NULL;

                    // optimization: read the first record directly into result
                    result = var->read(0, result);

                    // ignore character data
                    if (result->get_type() == DataType::CHAR) {
                        writer->write(result, var->get_name(), 0);
                        delete result;
                        continue;
                    }

                    if (var->has_fill_value(0)) {
                        tally = Array::create(DataType::INT, result->get_shape());
                        init_tally(result, tally, var->get_fill_value(0));
                    }

                    if (op == OP_RMS || op == OP_RMSSDN || op == OP_AVGSQR) {
                        // square the values
                        result->imul(result);
                    }

                    // read the rest of the records
                    for (int64_t rec=1; rec<nrec; ++rec) {
                        array = var->read(rec, array);
                        if (op == OP_MAX) {
                            result->imax(array);
                        }
                        else if (op == OP_MIN) {
                            result->imin(array);
                        }
                        else {
                            if (op == OP_RMS
                                    || op == OP_RMSSDN
                                    || op == OP_AVGSQR) {
                                // square the values
                                array->imul(array);
                            }
                            // sum the values or squares, keep a tally
                            result->set_counter(tally);
                            result->iadd(array);
                            result->set_counter(NULL);
                        }
                    }

                    // normalize, multiply, etc where necessary
                    if (op == OP_AVG || op == OP_SQRT || op == OP_SQRAVG
                            || op == OP_RMS || op == OP_RMS || op == OP_AVGSQR) {
                        if (NULL != tally) {
                            result->idiv(tally);
                        } else {
                            ScalarArray scalar(result->get_type());
                            scalar.fill_value(nrec);
                            result->idiv(&scalar);
                        }
                    }
                    else if (op == OP_RMSSDN) {
                        if (NULL != tally) {
                            ScalarArray scalar(tally->get_type());
                            scalar.fill_value(1);
                            tally->isub(&scalar);
                            result->idiv(tally);
                        } else {
                            ScalarArray scalar(result->get_type());
                            scalar.fill_value(nrec-1);
                            result->idiv(&scalar);
                        }
                    }

                    // some operations require additional processing */
                    if (op == OP_RMS || op == OP_RMSSDN || op == OP_SQRT) {
                        result->ipow(0.5);
                    }
                    else if (op == OP_SQRAVG) {
                        result->imul(result);
                    }

                    writer->write(result, var->get_name(), 0);
                    delete result;
                    delete array;
                    if (NULL != tally) {
                        delete tally;
                    }
                }
                else {
                    Array *array = var->read();
                    writer->write(array, var->get_name());
                    delete array;
                }
            }
        }

        // clean up
        delete dataset;
        delete masks;
        delete writer;

        pagoda::finalize();
    }
    catch (CommandException &ex) {
        if (dataset) {
            delete dataset;
        }
        if (masks) {
            delete masks;
        }
        if (writer) {
            delete writer;
        }
        pagoda::println_zero(ex.what());
        pagoda::finalize();
        return EXIT_FAILURE;
    }
    catch (PagodaException &ex) {
        pagoda::abort(ex.what());
    }
    catch (exception &ex) {
        pagoda::abort(ex.what());
    }

    return EXIT_SUCCESS;
}


void init_tally(Array *result, Array *tally, double fill)
{
    void *ptr_result;
    int *ptr_tally;
    DataType type = result->get_type();
    
    // result and tally arrays are assumed to have same distributions
    ASSERT(result->same_distribution(tally));

    ptr_result = result->access();
    ptr_tally = static_cast<int*>(tally->access());
    if (NULL != ptr_result) {
        ASSERT(NULL != ptr_tally);
#define DATATYPE_EXPAND(DT,T) \
        if (DT == type) { \
            T *data = static_cast<T*>(ptr_result); \
            T _fill = static_cast<T>(fill); \
            for (int64_t i=0,limit=result->get_local_size(); i<limit; ++i) { \
                ptr_tally[i] = (data[i] == _fill) ? 0 : 1; \
            } \
        } else
#include "DataType.def"
        result->release();
        tally->release_update();
    }
}

