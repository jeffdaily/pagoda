#ifndef PNETCDFDIMENSION_H_
#define PNETCDFDIMENSION_H_

#include <iostream>
#include <string>

using std::ostream;
using std::string;

#include "Dimension.H"

class Dataset;
class PnetcdfDataset;


/**
 * The pnetcdf implementation of the Dimension.
 */
class PnetcdfDimension : public Dimension
{
    public:
        PnetcdfDimension(PnetcdfDataset *dataset, int dimid);
        virtual ~PnetcdfDimension();

        virtual string get_name() const;
        virtual int64_t get_size() const;
        virtual bool is_unlimited() const;
        virtual Dataset* get_dataset() const;

        PnetcdfDataset* get_netcdf_dataset() const;
        int get_id() const;

        virtual ostream& print(ostream &os) const;

    protected:
        PnetcdfDataset *dataset;
        int id;
        string name;
        int64_t size;
        bool unlimited;
};

#endif // PNETCDFDIMENSION_H_
