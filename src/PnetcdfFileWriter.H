#ifndef PNETCDFFILEWRITER_H_
#define PNETCDFFILEWRITER_H_

#include <stdint.h>

#include <string>
#include <map>
#include <vector>

#include <pnetcdf.h>

#include "AbstractFileWriter.H"
#include "FileFormat.H"

using std::string;
using std::map;
using std::vector;

class Dataset;
class PnetcdfVariable;
class Variable;

FileWriter* pagoda_pnetcdf_create(const string &, FileFormat);


/**
 * Writes a Dataset to a netcdf-3 file using pnetcdf.
 */
class PnetcdfFileWriter : public AbstractFileWriter
{
    public:
        static nc_type to_nc(const DataType &type);

        PnetcdfFileWriter(const string &filename, FileFormat format);
        virtual ~PnetcdfFileWriter();
        virtual void close();

        // named parameter idiom, but using virtuals
        virtual FileWriter* create();
        virtual FileWriter* fixed_record_dimension(int size);
        virtual FileWriter* header_pad(int size);
        virtual FileWriter* file_format(FileFormat format);
        virtual FileWriter* append(bool value);
        virtual FileWriter* overwrite(bool value);

        virtual void def_dim(const string &name, int64_t size);
        virtual void def_dim(Mask *mask);
        virtual void def_dim(Dimension *dim);

        virtual void def_var(const string &name,
                             const vector<string> &dims,
                             const DataType &type,
                             const vector<Attribute*> &atts);
        virtual void def_var(const string &name,
                             const vector<Dimension*> &dims,
                             const DataType &type,
                             const vector<Attribute*> &atts=vector<Attribute*>());
        virtual void def_var(const string &name,
                             const vector<Mask*> &dims,
                             const DataType &type,
                             const vector<Attribute*> &atts=vector<Attribute*>());
        virtual void def_var(Variable *var);

        virtual void write_att(const string &name, Values *values,
                               DataType type,
                               const string &var_name=string(""));
        virtual void write_att(Attribute *att, const string &name=string(""));

        virtual void write(Array *array, const string &name);
        virtual void write(Array *array, const string &name,
                           int64_t record);
        virtual void write(Array *array, const string &name,
                           const vector<int64_t> &start);

        virtual void iwrite(Array *array, const string &name);
        virtual void iwrite(Array *array, const string &name,
                            int64_t record);
        virtual void iwrite(Array *array, const string &name,
                            const vector<int64_t> &start);
        virtual void wait();

        virtual ostream& print(ostream &os) const;

    protected:
        int get_dim_id(const string &name) const;
        int64_t get_dim_size(const string &name) const;
        int get_var_id(const string &name) const;
        vector<int> get_var_dims(const string &name) const;
        vector<int64_t> get_var_shape(const string &name) const;

#if 0
        void def_check() const;
#endif
        void maybe_redef();
        void maybe_enddef();

        void write_att_id(Attribute *attr, int varid);
        void write_atts_id(const vector<Attribute*> &atts, int varid);
        void write(int handle, int id, int record=-1);

        void write_wrapper(Array *array, const string &name, bool nonblocking);
        void write_wrapper(Array *array, const string &name,
                           int64_t record, bool nonblocking);
        void write_wrapper(Array *array, const string &name,
                           const vector<int64_t> &start, bool nonblocking);
        void do_write(Array *array, int varid, const vector<MPI_Offset> &start,
                      const vector<MPI_Offset> &count, bool nonblocking);

        bool is_in_define_mode;
        string filename;
        int ncid;
        int unlimdimid;

        int _fixed_record_dimension;
        int _header_pad;
        FileFormat _file_format;
        bool _append;
        bool _overwrite;

        map<string,int>     dim_id;
        map<string,int64_t> dim_size;

        map<string,int>              var_id;
        map<string,vector<int> >     var_dims;
        map<string,vector<int64_t> > var_shape;

        vector<int> nb_requests;
        vector<Array*> nb_arrays_to_release;
        vector<void*> nb_buffers_to_delete;

        bool open;
};

#endif // PNETCDFFILEWRITER_H_
