#ifndef NETCDF4VARIABLE_H_
#define NETCDF4VARIABLE_H_

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
class Netcdf4Attribute;
class Netcdf4Dimension;
class Netcdf4Dataset;


/**
 * The netcdf4 implementation of the Variable.
 */
class Netcdf4Variable : public AbstractVariable
{
    public:
        Netcdf4Variable(Netcdf4Dataset *dataset, int varid);
        virtual ~Netcdf4Variable();

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

        Netcdf4Dataset* get_netcdf_dataset() const;
        int get_id() const;

        friend class Netcdf4Dataset;

    protected:
        Array* read_wrapper(Array *dst) const;
        Array* read_wrapper(int64_t record, Array *dst) const;
        bool find_bit(const vector<Dimension*> &adims,
                      const vector<int64_t> &lo, const vector<int64_t> &hi) const;
        void do_read(Array *dst, const vector<size_t> &start,
                     const vector<size_t> &count, bool found_bit) const;

        virtual bool needs_renumber() const;
        virtual void renumber(Array *array) const;

        Netcdf4Dataset *dataset;
        int id;
        string name;
        vector<Netcdf4Dimension*> dims;
        vector<Netcdf4Attribute*> atts;
        DataType type;
};

#endif // NETCDF4VARIABLE_H_
