#ifdef HAVE_CONFIG_H
#   include <config.h>
#endif

#include <iostream>
#include <string>
#include <map>

#include <ga.h>

#include "Aggregation.H"
#include "AggregationJoinExisting.H"
#include "AggregationUnion.H"
#include "Dataset.H"
#include "Debug.H"
#include "Dimension.H"
#include "DistributedMask.H"
#include "FileWriter.H"
#include "Pack.H"
#include "SubsetterCommands.H"
#include "SubsetterException.H"
#include "Util.H"
#include "Variable.H"

using std::cout;
using std::endl;
using std::map;
using std::string;

typedef map<string,int> SumMap;
typedef map<string,int>::iterator SumMapIter;

static void subset(Variable *var, FileWriter *writer, SumMap &sum_map);
static void subset_record(Variable *var, FileWriter *writer, SumMap &sum_map);

#ifdef F77_DUMMY_MAIN
#  ifdef __cplusplus
     extern "C"
#  endif
   int F77_DUMMY_MAIN() { return 1; }
#endif


int main(int argc, char **argv)
{
    MPI_Init(&argc, &argv);
    GA_Initialize();
    TRACER("AFTER INITS\n");

    SubsetterCommands cmd;
    Dataset *dataset;
    Aggregation *agg;
    vector<Variable*> vars;
    FileWriter *writer;
    SumMap sum_map;

    TRACER("cmd line parsing BEGIN\n");
    try {
        cmd.parse(argc,argv);
    }
    catch (SubsetterException &ex) {
        if (ME == 0) {
            cout << ex.what() << endl;
        }
        exit(EXIT_FAILURE);
    }
    TRACER("cmd line parsing END\n");

    const vector<string> &infiles = cmd.get_intput_filenames();
    if (cmd.get_join_name().empty()) {
        dataset = agg = new AggregationUnion;
    } else {
        dataset = agg = new AggregationJoinExisting(cmd.get_join_name());
    }
    for (size_t i=0,limit=infiles.size(); i<limit; ++i) {
        TRACER1("adding %s\n", infiles[i].c_str());
        agg->add(Dataset::open(infiles[i]));
    }

    Util::calculate_required_memory(dataset->get_vars());

    dataset->create_masks();
    dataset->adjust_masks(cmd.get_slices());
    dataset->adjust_masks(cmd.get_box());

    vars = dataset->get_vars();
    writer = FileWriter::create(cmd.get_output_filename());
    writer->copy_atts(dataset->get_atts());
    writer->def_dims(dataset->get_dims());
    writer->def_vars(vars);
    TRACER("pre-create the partial sums BEGIN\n");
    for (vector<Variable*>::iterator it=vars.begin(); it!=vars.end(); ++it) {
        Variable *var = *it;
        vector<Dimension*> dims = var->get_dims();
        if (var->has_record()) {
            if (var->num_dims() == 1) {
                continue; // skip record coordinate i.e. "time"
            }
            dims.erase(dims.begin());
        }
        for (vector<Dimension*>::iterator dimit=dims.begin();
                dimit!=dims.end(); ++dimit) {
            Dimension *dim = *dimit;
            string name = dim->get_name();
            DistributedMask *mask;
            mask = dynamic_cast<DistributedMask*>(dim->get_mask());
            if (!mask) {
                continue;
            }
            if (sum_map.end() == sum_map.find(name)) {
                int64_t size = dim->get_size();
#if 8 == SIZEOF_INT
                int matype = C_INT;
#elif 8 == SIZEOF_LONG
                int matype = C_LONG;
#elif 8 == SIZEOF_LONG_LONG
                int matype = C_LONGLONG;
#endif
                int handle = 
                    NGA_Create64(matype, 1, &size, "partial_sum", NULL);
                TRACER2("created partial sum of %s handle=%d\n",
                        name.c_str(), handle);
                sum_map[name] = handle;
                partial_sum(mask->get_handle(), handle, 0);
            }
        }
    }
    TRACER("pre-create the partial sums END\n");
    for (vector<Variable*>::iterator it=vars.begin(); it!=vars.end(); ++it) {
        Variable *var = *it;
        if (var->has_record() && var->num_dims() > 1) {
            subset_record(var, writer, sum_map);
        } else {
            subset(var, writer, sum_map);
        }
    }

    delete writer; // also closes file
    TRACER("after delete writer\n");
    delete dataset;
    TRACER("after delete dataset\n");

    GA_Terminate();
    MPI_Finalize();
    exit(EXIT_SUCCESS);
}


// subset and write var
void subset(Variable *var, FileWriter *writer, SumMap &sum_map)
{
    const string name = var->get_name();
    TRACER1("SubsetterMain::subset %s BEGIN\n", name.c_str());

    var->read();
    var->reindex(); // noop if not ConnectivityVariable

    if (var->needs_subset()) {
        int ga_out;
        size_t ndim = var->num_dims();
        int ga_masks[ndim];
        int ga_masksums[ndim];
        vector<int64_t> shape_out = var->get_sizes(true);
        int matype = var->get_type().as_ma();
        vector<Dimension*> dims = var->get_dims();

        // HACK for now since all masks are distributed by default
        for (size_t dimidx=0; dimidx<ndim; ++dimidx) {
            Dimension *dim = dims.at(dimidx);
            SumMapIter iter = sum_map.find(dim->get_name());
            DistributedMask *mask;
            mask = dynamic_cast<DistributedMask*>(dim->get_mask());
            if (!mask || sum_map.end() == iter) {
                GA_Error("!mask || sum_map.end()", 0);
            }
            ga_masks[dimidx] = mask->get_handle();
            ga_masksums[dimidx] = iter->second;
            TRACER3("%zd mask handle=%d, masksum handle=%d\n",
                    dimidx, ga_masks[dimidx], ga_masksums[dimidx]);
        }

        TRACER("before ga_out (pack_dst) create\n");
        ga_out = NGA_Create64(matype, ndim, &shape_out[0], "pack_dst", NULL);
        pack(var->get_handle(), ga_out, ga_masks, ga_masksums);
        writer->write(ga_out, name);
        GA_Destroy(ga_out);
    } else {
        TRACER("no masks, so a direct copy\n");
        writer->write(var->get_handle(), name);
    }

    var->release_handle();
    TRACER1("SubsetterMain::subset %s END\n", name.c_str());
}


// subset and write record var
// TODO what if there is no mask (needs_subset() == false)??  Need if/else block
void subset_record(Variable *var, FileWriter *writer, SumMap &sum_map)
{
    const string name = var->get_name();
    TRACER1("SubsetterMain::subset_record %s BEGIN\n", name.c_str());
    vector<Dimension*> dims = var->get_dims();
    size_t ndim = dims.size();
    size_t ndim_1 = ndim - 1;
    vector<int> mask_rec;
    int ga_masks[ndim-1];
    int ga_masksums[ndim-1];
    vector<int64_t> shape_out = var->get_sizes(true);
    int ga_out;
    int matype = var->get_type().as_ma();

    if (dims[0]->get_mask()) {
        dims[0]->get_mask()->get_data(mask_rec);
    } else {
        mask_rec.assign(dims[0]->get_size(), 1);
    }

    // HACK for now since all masks are distributed by default
    for (size_t dimidx=1; dimidx<ndim; ++dimidx) {
        Dimension *dim = dims.at(dimidx);
        SumMapIter iter = sum_map.find(dim->get_name());
        DistributedMask *mask;
        mask = dynamic_cast<DistributedMask*>(dim->get_mask());
        if (!mask || sum_map.end() == iter) {
            GA_Error("!mask || sum_map.end()", 0);
        }
        ga_masks[dimidx-1] = mask->get_handle();
        ga_masksums[dimidx-1] = iter->second;
        TRACER3("%zd mask handle=%d, masksum handle=%d\n",
                dimidx, ga_masks[dimidx-1], ga_masksums[dimidx-1]);
    }

    ga_out = NGA_Create64(matype, ndim_1, &shape_out[1], "pack_dst", NULL);

    for (int64_t record=0,nrec=dims[0]->get_size(),packed_recidx=0; record<nrec; ++record) {
        if (mask_rec[record] != 0) {
            var->set_record_index(record);
            var->read();
            pack(var->get_handle(), ga_out, &ga_masks[0], &ga_masksums[0]);
            writer->write(ga_out, name, packed_recidx++);
        }
    }

    GA_Destroy(ga_out);
    var->release_handle();
    TRACER1("SubsetterMain::subset_record %s END\n", name.c_str());
}
