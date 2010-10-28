#ifndef AGGREGATION_H_
#define AGGREGATION_H_

#include <vector>

using std::vector;

#include "AbstractDataset.H"

class Dataset;


/**
 * Abstract base class for an aggregated Dataset.
 *
 * The types of aggregations supported are described at
 * http://www.unidata.ucar.edu/software/netcdf/ncml/v2.2/Aggregation.html
 * and are useful to logically represent multiple files as a single Dataset.
 *
 * MaskMaps are shared among all Dataset instances aggregated by an
 * Aggregation instance.
 *
 * What it means to Aggregation::add() a Dataset is up to the specific
 * Aggregation implementation.
 */
class Aggregation : public AbstractDataset
{
    public:
        /**
         * Destructs an Aggregation.
         */
        virtual ~Aggregation();

        /**
         * Explicitly closes an Aggregation and all Dataset istances within.
         */
        virtual void close();

        /**
         * Adds a single Dataset to this Aggregation.
         *
         * @param[in] dataset the Dataset to add
         */
        virtual void add(Dataset *dataset) = 0;

        /**
         * Adds multiple Dataset instances to this Aggregation.
         *
         * @param[in] datasets the Dataset instances to add
         */
        virtual void add(const vector<Dataset*> &datasets);

        /**
         * Returns all Dataset instances aggregated here.
         *
         * @return all Dataset instances aggregated here
         */
        virtual vector<Dataset*> get_datasets() const = 0;

        /**
         * @copybrief Dataset::set_masks(MaskMap*)
         * @copydetails Dataset::set_masks(MaskMap*)
         * Also shares the MaskMap instance with all aggregated Dataset
         * instances.
         */
        virtual void set_masks(MaskMap *masks);

        /**
         * @copybrief Dataset::push_masks(MaskMap*)
         * @copydetails Dataset::push_masks(MaskMap*)
         * Also shares the MaskMap instance with all aggregated Dataset
         * instances.
         */
        virtual void push_masks(MaskMap *masks);

        /**
         * @copybrief Dataset::pop_masks()
         * @copydetails Dataset::pop_masks()
         * Also pops the MaskMap instance from all aggregated Dataset
         * instances.
         */
        virtual MaskMap* pop_masks();

    protected:
        /**
         * Constructs an Aggregation abstract base class.
         */
        Aggregation();
};

#endif // AGGREGATION_H_
