#include <string>

using std::string;

class Hints
{
    public:
        static string striping_unit;
        static string cb_buffer_size;
        static string romio_cb_read;
        static string romio_ds_read;

        static MPI_Info get_info();
        static string to_string();
};
