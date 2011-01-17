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
        Array* _read(Array *dst) const;
        Array* _read(int64_t record, Array *dst) const;
        void do_read(Array *dst, const vector<MPI_Offset> &start,
                     const vector<MPI_Offset> &count, bool found_bit) const;

        Array* _iread(Array *dst);
        Array* _iread(int64_t record, Array *dst);
        void do_iread(Array *dst, const vector<MPI_Offset> &start,
                      const vector<MPI_Offset> &count, bool found_bit);
        void after_wait();

        Array* read_prep(Array *dst,
                vector<MPI_Offset> &start, vector<MPI_Offset> &count,
                bool &found_bit) const;
        Array* read_prep(Array *dst,
                vector<MPI_Offset> &start, vector<MPI_Offset> &count,
                bool &found_bit, int64_t record) const;
        bool find_bit(const vector<Dimension*> &adims,
                      const vector<int64_t> &lo,
                      const vector<int64_t> &hi) const;

        virtual bool needs_renumber() const;
        virtual void renumber(Array *array) const;

        PnetcdfDataset *dataset;
        int id;
        string name;
        vector<PnetcdfDimension*> dims;
        vector<PnetcdfAttribute*> atts;
        DataType type;

        vector<int> nb_requests; /**< non-blocking io requests */
        vector<void*> nb_buffers; /**< when type != read_type */
        vector<Array*> nb_arrays_to_release;
        vector<Array*> nb_arrays_to_pack;
};

#endif // PNETCDFVARIABLE_H_
