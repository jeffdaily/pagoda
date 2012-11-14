#if HAVE_CONFIG_H
#   include <config.h>
#endif

#include <stdint.h>

#include <algorithm>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <map>
#include <stdexcept>
#include <string>
#include <utility>
#include <vector>

#include "Array.H"
#include "Bootstrap.H"
#include "CommandException.H"
#include "CommandLineOption.H"
#include "Dataset.H"
#include "Dimension.H"
#include "FileWriter.H"
#include "Grid.H"
#include "MaskMap.H"
#include "NotImplementedException.H"
#include "Numeric.H"
#include "PgwaCommands.H"
#include "Print.H"
#include "ScalarArray.H"
#include "Util.H"
#include "Validator.H"
#include "Variable.H"

using std::exception;
using std::map;
using std::make_pair;
using std::pair;
using std::sort;
using std::string;
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

#define STR_VEC(vec) pagoda::vec_to_string(vec,",",#vec)

static void pgwa_blocking(Dataset *dataset, const vector<Variable*> &vars,
        FileWriter *writer, const string &op, PgwaCommands &cmd);
static vector<int64_t> conformity(
        vector<Dimension*> lhs, vector<Dimension*> rhs);
static void get_corresponding(
        vector<int64_t> lo, vector<int64_t> hi, vector<int64_t> dim_map,
        Array *array, void* &data, vector<int64_t> &shape);


int main(int argc, char **argv)
{
    PgwaCommands cmd;
    Dataset *dataset = NULL;
    vector<Variable*> vars;
    FileWriter *writer = NULL;
    string op;

    try {
        pagoda::initialize(&argc, &argv);

        Variable::promote_to_float = true;

        cmd.parse(argc,argv);
        cmd.get_inputs_and_outputs(dataset, vars, writer);
        op = cmd.get_operator();

        if (cmd.is_nonblocking()) {
            if (cmd.is_reading_all_records()) {
                ASSERT(0);
            } else {
                if (cmd.get_number_of_groups() > 1) {
                    ASSERT(0);
                } else {
                    ASSERT(0);
                }
            }
        } else {
            if (cmd.get_number_of_groups() > 1) {
                ASSERT(0);
            } else {
                pgwa_blocking(dataset, vars, writer, op, cmd);
            }
        }

        // clean up
        if (dataset) {
            delete dataset;
        }
        if (writer) {
            delete writer;
        }

        pagoda::finalize();
    }
    catch (CommandException &ex) {
        if (dataset) {
            delete dataset;
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


void pgwa_blocking(Dataset *dataset, const vector<Variable*> &vars,
        FileWriter *writer, const string &op, PgwaCommands &cmd)
{
    vector<Variable*>::const_iterator var_it;
    set<string> reduced_dims = cmd.get_reduced_dimensions();
    Variable *var_mask = cmd.get_mask_variable();
    Variable *var_weight = cmd.get_weight_variable();

    // read each variable in order
    // for record variables, read one record at a time
    for (var_it=vars.begin(); var_it!=vars.end(); ++var_it) {
        Variable *var = *var_it;
        if (cmd.is_verbose()) {
            pagoda::println_zero("processing variable: " + var->get_name());
        }
        if (var->has_record() && var->get_nrec() > 0) {
            int64_t nrec = var->get_nrec();
            Array *result = NULL;
            Array *array = NULL; // reuse allocated array each record

            /* -- steps --
             * - are the mask dimensions compatible with the current var?
             * -- get variable mask as a bitmask
             * -- transpose bitmask if needed
             */


            array = var->read(0, array);
            if (cmd.is_verbose()) {
                pagoda::println_zero("\tfinished reading record 0");
            }

            if (reduced_dims.empty() && op == OP_AVG) {
                result = array->reduce_add();
            } else {
                throw NotImplementedException("TODO");
            }

            // read the rest of the records
            for (int64_t rec=1; rec<nrec; ++rec) {
                array = var->read(rec, array);
                if (cmd.is_verbose()) {
                    pagoda::println_zero("\tfinished reading record "
                            + pagoda::to_string(rec));
                }

                result->iadd(array->reduce_add());
            }

            ScalarArray tally(result->get_type());
            tally.fill_value(nrec);
            result->idiv(&tally);

            writer->write(result, var->get_name());
            if (cmd.is_verbose()) {
                pagoda::println_zero("\tfinished writing");
            }
            delete result;
            delete array;
        }
        else {
            Array *array = var->read();
            Array *mask = NULL;
            Array *missing = NULL;
            Array *weight = NULL;
            vector<int64_t> dim_map_mask;
            vector<int64_t> dim_map_weight;

            ASSERT(NULL != array);

            if (var_weight) {
                weight = var_weight->read();
                dim_map_weight = conformity(
                        var_weight->get_dims(), var->get_dims());
            }

            if (var_mask) {
                Array *raw_mask = var_mask->read();
                mask = raw_mask->get_mask();
                dim_map_mask = conformity(
                        var_mask->get_dims(), var->get_dims());
            }

            if (array->has_validator()) {
                missing = array->get_mask();
            }

            void *data = array->access();
            void *data_mask = NULL;
            void *data_missing = NULL;
            void *data_weight = NULL;
            vector<int64_t> shape_mask;
            vector<int64_t> shape_weight;
            if (NULL != data) {
                vector<int64_t> lo,hi;
                array->get_distribution(lo,hi);
                // get corresponding portion of missing value mask
                if (missing) {
                    data_missing = missing->get(lo,hi);
                }
                // get corresponding portion of weight
                if (weight) {
                    get_corresponding(lo, hi, dim_map_weight, weight,
                            data_weight, shape_weight);
                }
                // get corresponding portion of mask
                if (mask) {
                    get_corresponding(lo, hi, dim_map_mask, mask,
                            data_mask, shape_mask);
                }
#if 1
                pagoda::println_sync(STR_VEC(dim_map_weight));
                pagoda::println_sync(STR_VEC(shape_weight));
                pagoda::println_sync(STR_VEC(dim_map_mask));
                pagoda::println_sync(STR_VEC(shape_mask));
#endif

                if (weight && mask && missing) {
                    throw NotImplementedException("TODO weight mask missing");
                }
                else if (weight && mask) {
                    throw NotImplementedException("TODO weight mask");
                }
                else if (weight && missing) {
                    throw NotImplementedException("TODO weight missing");
                }
                else if (mask && missing) {
                    throw NotImplementedException("TODO mask missing");
                }
                else if (weight) {
                    float dst=0;
                    pagoda::reduce_sum(array->get_type(), 
                            data, array->get_local_shape(),
                            &dst, vector<int64_t>(array->get_ndim(),0),
                            data_weight, shape_weight);
                    pagoda::println_sync("dst=" + pagoda::to_string(dst));
                }
                else if (mask) {
                    throw NotImplementedException("TODO mask");
                }
                else if (missing) {
                    throw NotImplementedException("TODO missing");
                }
                else {
                    throw NotImplementedException("shouldn't happen");
                }
            }

            //writer->write(result, var->get_name());

            delete array;
        }
    }
}


/* Map lhs Dimension instances to rhs Dimension instances. */
static vector<int64_t> conformity(
        vector<Dimension*> lhs, vector<Dimension*> rhs)
{
    vector<int64_t> result;

    for (size_t i=0, i_limit=lhs.size(); i < i_limit; ++i) {
        for (size_t j=0, j_limit=rhs.size(); j < j_limit; ++j) {
            if (Dimension::equal(lhs[i],rhs[j])) {
                result.push_back(j);
                break;
            }
        }
    }

    return result;
}


static bool sort_helper(
        const pair<int64_t,int64_t> &i,
        const pair<int64_t,int64_t> &j)
{
    return i.first < j.first;
}


static void get_corresponding(
        vector<int64_t> lo, vector<int64_t> hi, vector<int64_t> dim_map,
        Array *array, void* &data, vector<int64_t> &shape)
{
    vector<int64_t> local_shape = pagoda::get_shape(lo, hi);
    vector<int64_t> alo, ahi;
    bool need_transpose = false;

    /* the given array may have fewer dimensions than the data array, or the
     * dimensions may occur in a permuted order, so we use the given dimension
     * map to get the corresponding data from the given array */
    for (size_t i=0; i<dim_map.size(); ++i) {
        ASSERT(dim_map[i] < lo.size() && dim_map[i] >= 0);
        alo.push_back(lo[dim_map[i]]);
        ahi.push_back(hi[dim_map[i]]);
        if (i > 0) {
            if (dim_map[i] < dim_map[i-1]) {
                need_transpose = true;
            }
        }
    }

    data = array->get(alo, ahi);

    if (need_transpose) {
        void *old_data = data;
        vector<pair<int64_t,int64_t> > helper;
        vector<int64_t> axes;
        DataType type = array->get_type();

#if 1
        pagoda::println_zero("need transpose");
#endif

        // the transpose function requires as input a vector indicating how to
        // reorder the axes.
        for (size_t i=0; i<dim_map.size(); ++i) {
            helper.push_back(make_pair(dim_map[i],i));
        }
        sort(helper.begin(), helper.end(), sort_helper);
        for (size_t i=0; i<dim_map.size(); ++i) {
            axes.push_back(helper[i].second);
        }

        // allocate a buffer to store the result of the transposition
        data = pagoda::allocate(type, local_shape);
        pagoda::transpose(type, old_data, local_shape, data, axes);
        pagoda::deallocate(type, old_data);
    }

    // sort the dim_map so we can insert zeros into the shape to establish the
    // broadcast shape
    sort(dim_map.begin(), dim_map.end());
    shape = vector<int64_t>(lo.size(), 0);
#if 1
    pagoda::println_sync(STR_VEC(dim_map));
    pagoda::println_sync(STR_VEC(shape));
    pagoda::println_sync(STR_VEC(local_shape));
#endif
    for (size_t i=0; i<dim_map.size(); ++i) {
        shape[dim_map[i]] = local_shape[dim_map[i]];
    }
}

