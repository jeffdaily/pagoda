#ifndef PNETCDFATTRIBUTE_H_
#define PNETCDFATTRIBUTE_H_

#include <iostream>
#include <string>

using std::ostream;
using std::string;

#include "Attribute.H"
#include "DataType.H"

class Dataset;
class PnetcdfDataset;
class PnetcdfVariable;
class Values;
class Variable;


/**
 * The pnetcdf implementation of an Attribute.
 */
class PnetcdfAttribute : public Attribute
{
    public:
        PnetcdfAttribute(PnetcdfDataset *dataset, int attid,
                PnetcdfVariable *var=NULL);
        virtual ~PnetcdfAttribute();

        virtual string get_name() const;
        virtual DataType get_type() const;
        virtual Values* get_values() const;

        virtual Dataset* get_dataset() const;
        virtual Variable* get_var() const;

        PnetcdfDataset* get_netcdf_dataset() const;
        PnetcdfVariable* get_netcdf_var() const;
        int get_id() const;

    protected:
        PnetcdfDataset *dataset;
        PnetcdfVariable *var;
        int id;
        string name;
        DataType type;
        Values *values;
};

#endif // PNETCDFATTRIBUTE_H_
