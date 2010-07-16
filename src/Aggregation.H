#ifndef AGGREGATION_H_
#define AGGREGATION_H_

#include <vector>

using std::vector;

#include "AbstractDataset.H"

class Dataset;


/**
 * Interface for an aggregated Dataset.
 */
class Aggregation : public AbstractDataset
{
    public:
        virtual ~Aggregation();
        virtual void close();

        virtual void add(Dataset *dataset) = 0;
        virtual void add(const vector<Dataset*> &datasets);
        virtual vector<Dataset*> get_datasets() const = 0;

        virtual void set_masks(MaskMap *masks);
        virtual void push_masks(MaskMap *masks);
        virtual MaskMap* pop_masks();

    protected:
        Aggregation();
};

#endif // AGGREGATION_H_
