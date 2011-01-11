#ifndef PNETCDFVARIABLE_H_
#define PNETCDFVARIABLE_H_

#include <stdint.h>

#include <iostream>
#include <string>
#include <vector>

using std::ostream;
using std::string;
using std::vector;

#include "AbstractVariable.H"
#include "DataType.H"

class Attribute;
class Dataset;
class Dimension;
class PnetcdfAttribute;
class PnetcdfDimension;
class PnetcdfDataset;


/**
 * The pnetcdf implementation of the Variable.
 */
class PnetcdfVariable : public AbstractVariable
{
    public:
        PnetcdfVariable(PnetcdfDataset *dataset, int varid);
        virtual ~PnetcdfVariable();

        // virtuals from AbstractVariable
        virtual string get_name() const;
        virtual vector<Dimension*> get_dims() const;
        virtual vector<Attribute*> get_atts() const;
        virtual Dataset* get_dataset() const;
        virtual DataType get_type() const;
        virtual Array* read(Array *dst=NULL) const;
        virtual Array* read(int64_t record, Array *dst=NULL) const;
        virtual Array* iread(Array *dst=NULL);
        virtual Array* iread(int64_t record, Array *dst=NULL);

        virtual ostream& print(ostream &os) const;

        PnetcdfDataset* get_netcdf_dataset() const;
        int get_id() const;

        friend class PnetcdfDataset;

    protected:
        Array* read(Array *dst, bool nonblocking) const;
        Array* read(int64_t record, Array *dst, bool nonblocking) const;
        bool find_bit(const vector<Dimension*> &adims,
                      const vector<int64_t> &lo, const vector<int64_t> &hi) const;
        void do_read(Array *dst, const vector<MPI_Offset> &start,
                     const vector<MPI_Offset> &count, bool found_bit,
                     bool nonblocking) const;

        virtual bool needs_renumber() const;
        virtual void renumber(Array *array) const;

        PnetcdfDataset *dataset;
        int id;
        string name;
        vector<PnetcdfDimension*> dims;
        vector<PnetcdfAttribute*> atts;
        DataType type;
};

#endif // PNETCDFVARIABLE_H_
