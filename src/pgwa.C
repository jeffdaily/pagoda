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

using std::bind2nd;
using std::count_if;
using std::exception;
using std::greater;
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

    if (cmd.is_verbose()) {
        pagoda::println_zero("op: " + op);
    }

    // read each variable in order
    // for record variables, read one record at a time
    for (var_it=vars.begin(); var_it!=vars.end(); ++var_it) {
        Variable *var = *var_it;
        if (cmd.is_verbose()) {
            pagoda::println_zero("processing variable: " + var->get_name());
        }
        /* TODO memory efficient impl one record at a time */
#if 0
        if (var->has_record() && var->get_nrec() > 0) {
            int64_t nrec = var->get_nrec();
            Array *result = NULL;
            Array *array = NULL; // reuse allocated array each record

            /* -- steps --
             * - are the mask dimensions compatible with the current var?
             * -- get variable mask as a bitmask
             * -- transpose bitmask if needed
             */

            if (cmd.is_verbose()) {
                pagoda::println_zero("\trecord variable");
            }

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
        else
#endif
        {
            Array *numerator = NULL;
            Array *denominator = NULL;
            Array *array = var->read();
            Array *missing = array->get_mask();
            vector<Dimension*> var_dims = var->get_dims();
            vector<int64_t> reduced_shape = array->get_shape();
            int64_t original_ndim = array->get_ndim();
            bool needs_reduction = false;
            bool weight_ok = true;
            string the_op = op;

            if (cmd.is_verbose()) {
                pagoda::println_zero("\tnon-record variable");
            }

            ASSERT(NULL != array);

            // coordinate variables are always averaged regardless of OP
            if (var->is_coordinate()) {
                the_op = OP_AVG;
            }

            /* we need an array with the same type as the source array to hold
             * our results a we go; this will be the denominator */
            denominator = array->clone();
            denominator->fill_value(1);

            if (var_weight) {
                bool needs_transpose = false;
                bool needs_broadcast = false;
                vector<Dimension*> weight_dims = var_weight->get_dims();
                vector<int64_t> T_axes;
                int64_t T_index = 0;
                vector<int64_t> broadcast_shape = var->get_shape();

                /* Does the weight var have a subset of the same dimensions as
                 * the current var?
                 * While we're at it, let's construct the transpose axes as
                 * well as the possible broadcast shape. */
                if (weight_ok) {
                    T_index = 0;
                    T_axes.assign(weight_dims.size(), -1);
                    for (int64_t i=0; i<var_dims.size(); ++i) {
                        bool found_dim = false;
                        Dimension *var_dim = var_dims.at(i);
                        for (int64_t j=0; j<weight_dims.size(); ++j) {
                            Dimension *weight_dim = weight_dims.at(j);
                            if (Dimension::equal(weight_dim, var_dim)) {
                                T_axes.at(j) = T_index++;
                                found_dim = true;
                                break;
                            }
                        }
                        if (!found_dim) {
                            broadcast_shape.at(i) = 0;
                        }
                    }
                    /* Any -1 values in the transpose axes mean the weight var
                     * dims are not a subset of the current var dims and we
                     * skip the weight. */
                    /* We can also check whether a transpose is needed by
                     * looking for absense of monotonically increasing
                     * indicies. */
                    for (int64_t i=0; i<weight_dims.size(); ++i) {
                        if (T_axes.at(i) == -1) {
                            weight_ok = false;
                            break;
                        }
                        else if (i > 0 && T_axes.at(i) != T_axes[i-1]+1) {
                            needs_transpose = true;
                        }
                        else if (i == 0 && T_axes.at(i) != 0) {
                            needs_transpose = true;
                        }
                    }
                    /* We can also check whether a broadcasting operation is
                     * needed by looking for zeros in the broadcast_shape. */
                    for (int64_t i=0; i<var_dims.size(); ++i) {
                        if (broadcast_shape.at(i) <= 0) {
                            needs_broadcast = true;
                            break;
                        }
                    }
                }

                if (cmd.is_verbose()) {
                    pagoda::println_zero("\tweight ok? "
                            + pagoda::to_string(weight_ok));
                    pagoda::println_zero("\tneeds_transpose? "
                            + pagoda::to_string(needs_transpose));
                    pagoda::println_zero("\tneeds_broadcast? "
                            + pagoda::to_string(needs_broadcast));
                    pagoda::println_zero("\tT_axes="
                            + pagoda::vec_to_string(T_axes));
                    pagoda::println_zero("\tbroadcast_shape="
                            + pagoda::vec_to_string(broadcast_shape));
                }

                /* don't read weight var and process unless needed */
                if (weight_ok) {
                    Array *weight_maybe_transposed = NULL;
                    Array *weight = var_weight->read();
                    ASSERT(weight);
                    if (needs_transpose) {
                        weight_maybe_transposed = weight->transpose(T_axes);
                        ASSERT(weight_maybe_transposed);
                    }
                    else {
                        weight_maybe_transposed = weight;
                    }
                    if (needs_broadcast) {
                        denominator->imul(weight_maybe_transposed, broadcast_shape);
                    }
                    else {
                        denominator->imul(weight_maybe_transposed);
                    }
                    if (needs_transpose) {
                        delete weight_maybe_transposed;
                    }
                    delete weight;
                }
            }

            if (var_mask) {
                bool ok = true; /* to indicate dimension mismatches */
                bool needs_transpose = false;
                bool needs_broadcast = false;
                vector<Dimension*> mask_dims = var_mask->get_dims();
                vector<int64_t> T_axes;
                int64_t T_index = 0;
                vector<int64_t> broadcast_shape = var->get_shape();

                /* Does the mask var have a subset of the same dimensions as
                 * the current var?
                 * While we're at it, let's construct the transpose axes as
                 * well as the possible broadcast shape. */
                if (ok) {
                    T_index = 0;
                    T_axes.assign(mask_dims.size(), -1);
                    for (int64_t i=0; i<var_dims.size(); ++i) {
                        bool found_dim = false;
                        Dimension *var_dim = var_dims.at(i);
                        for (int64_t j=0; j<mask_dims.size(); ++j) {
                            Dimension *mask_dim = mask_dims.at(j);
                            if (Dimension::equal(mask_dim, var_dim)) {
                                T_axes.at(j) = T_index++;
                                found_dim = true;
                                break;
                            }
                        }
                        if (!found_dim) {
                            broadcast_shape.at(i) = 0;
                        }
                    }
                    /* Any -1 values in the transpose axes mean the mask var
                     * dims are not a subset of the current var dims and we
                     * skip the mask. */
                    /* We can also check whether a transpose is needed by
                     * looking for absense of monotonically increasing
                     * indicies. */
                    for (int64_t i=0; i<mask_dims.size(); ++i) {
                        if (T_axes.at(i) == -1) {
                            ok = false;
                            break;
                        }
                        else if (i > 0 && T_axes.at(i) != T_axes[i-1]+1) {
                            needs_transpose = true;
                        }
                        else if (i == 0 && T_axes.at(i) != 0) {
                            needs_transpose = true;
                        }
                    }
                    /* We can also check whether a broadcasting operation is
                     * needed by looking for zeros in the broadcast_shape. */
                    for (int64_t i=0; i<var_dims.size(); ++i) {
                        if (broadcast_shape.at(i) <= 0) {
                            needs_broadcast = true;
                            break;
                        }
                    }
                }

                if (cmd.is_verbose()) {
                    pagoda::println_zero("\tmask ok? "
                            + pagoda::to_string(ok));
                    pagoda::println_zero("\tneeds_transpose? "
                            + pagoda::to_string(needs_transpose));
                    pagoda::println_zero("\tneeds_broadcast? "
                            + pagoda::to_string(needs_broadcast));
                    pagoda::println_zero("\tT_axes="
                            + pagoda::vec_to_string(T_axes));
                    pagoda::println_zero("\tbroadcast_shape="
                            + pagoda::vec_to_string(broadcast_shape));
                }

                /* don't read mask var and process unless needed */
                if (ok) {
                    Array *mask_maybe_transposed = NULL;
                    Array *mask = var_mask->read();
                    ASSERT(mask);
                    if (needs_transpose) {
                        mask_maybe_transposed = mask->transpose(T_axes);
                        ASSERT(mask_maybe_transposed);
                    }
                    else {
                        mask_maybe_transposed = mask;
                    }
                    if (needs_broadcast) {
                        denominator->imul(mask_maybe_transposed, broadcast_shape);
                    }
                    else {
                        denominator->imul(mask_maybe_transposed);
                    }
                    if (needs_transpose) {
                        delete mask_maybe_transposed;
                    }
                    delete mask;
                }
            }

            /* mask values based on missing_value attribute */
            denominator->imul(missing);

            if (var_weight && weight_ok && the_op == OP_RMSSDN) {
                the_op = OP_RMS;
            }

            /* some ops need to square the input values */
            if (the_op == OP_AVGSQR || the_op == OP_RMS || the_op == OP_RMSSDN) {
                array->imul(array);
            }
            
            /* original array becomes the numerator */
            array->imul(denominator);

            /*
            OP_AVG);
            OP_SQRAVG);
            OP_AVGSQR);
            OP_MAX);
            OP_MIN);
            OP_RMS);
            OP_RMSSDN);
            OP_SQRT);
            OP_TTL);
            */

            /* what's the new shape of the array based on the dimensions we are
             * reducing? */
            int64_t reduced_ndim = 0;
            for (int64_t i=0; i<var_dims.size(); ++i) {
                Dimension *dim = var_dims.at(i);
                if (reduced_dims.empty()
                        || reduced_dims.count(dim->get_name())) {
                    reduced_shape[i] = 0;
                    needs_reduction = true;
                }
                else {
                    ++reduced_ndim;
                }
            }

            if (cmd.is_verbose()) {
                pagoda::println_zero("\treduced_shape="
                        + pagoda::vec_to_string(reduced_shape));
                pagoda::println_zero("\treduced_ndim="
                        + pagoda::to_string(reduced_ndim));
                pagoda::println_zero("\toriginal_ndim="
                        + pagoda::to_string(original_ndim));
                pagoda::println_zero("\tneeds_reduction="
                        + pagoda::to_string(needs_reduction));
            }

            Array *numerator_reduction = NULL;
            Array *denominator_reduction = NULL;

            if (the_op == OP_TTL || the_op == OP_MAX || the_op == OP_MIN) {
                /* reduce the numerator */
                if (needs_reduction) {
                    if (0 == reduced_ndim) {
                        if (the_op == OP_TTL) {
                            numerator_reduction = array->reduce_add();
                        }
                        else if (the_op == OP_MAX) {
                            numerator_reduction = array->reduce_max();
                        }
                        else if (the_op == OP_MIN) {
                            numerator_reduction = array->reduce_min();
                        }
                        else  {
                            ASSERT(false);
                        }
                    }
                    else {
                        if (the_op == OP_TTL) {
                            numerator_reduction = array->reduce_add(reduced_shape);
                        }
                        else if (the_op == OP_MAX) {
                            numerator_reduction = array->reduce_max(reduced_shape);
                        }
                        else if (the_op == OP_MIN) {
                            numerator_reduction = array->reduce_min(reduced_shape);
                        }
                        else  {
                            ASSERT(false);
                        }
                    }
                }
                else {
                    ASSERT(reduced_ndim == original_ndim);
                    numerator_reduction = array;
                    denominator_reduction = denominator;
                }
            }
            else {
                /* reduce the numerator and the denominator */
                if (needs_reduction) {
                    if (0 == reduced_ndim) {
                        numerator_reduction = array->reduce_add();
                        denominator_reduction = denominator->reduce_add();
                    }
                    else {
                        numerator_reduction = array->reduce_add(reduced_shape);
                        denominator_reduction = denominator->reduce_add(reduced_shape);
                    }
                }
                else {
                    ASSERT(reduced_ndim == original_ndim);
                    numerator_reduction = array;
                    denominator_reduction = denominator;
                }

                if (the_op == OP_RMSSDN) {
                    ScalarArray ONE(denominator_reduction->get_type());
                    ONE.fill_value(1);
                    denominator_reduction->isub(&ONE);
                }

                /* then divide */
                numerator_reduction->idiv(denominator_reduction);

                if (the_op == OP_SQRAVG) {
                    numerator_reduction->imul(numerator_reduction);
                }
                else if (the_op == OP_RMS || the_op == OP_RMSSDN) {
                    numerator_reduction->ipow(0.5);
                }
            }

            writer->write(numerator_reduction, var->get_name());

            //delete result;
            delete array;
            delete missing;
            delete denominator;
            if (needs_reduction) {
                delete denominator_reduction;
                delete numerator_reduction;
            }
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
            if (Dimension::equal(lhs.at(i),rhs.at(j))) {
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
        ASSERT(dim_map.at(i) < lo.size() && dim_map.at(i) >= 0);
        alo.push_back(lo[dim_map.at(i)]);
        ahi.push_back(hi[dim_map.at(i)]);
        if (i > 0) {
            if (dim_map.at(i) < dim_map[i-1]) {
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
            helper.push_back(make_pair(dim_map.at(i),i));
        }
        sort(helper.begin(), helper.end(), sort_helper);
        for (size_t i=0; i<dim_map.size(); ++i) {
            axes.push_back(helper.at(i).second);
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
        shape[dim_map.at(i)] = local_shape[dim_map.at(i)];
    }
}

