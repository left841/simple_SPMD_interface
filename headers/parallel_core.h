#ifndef __PARALLEL_CORE_H__
#define __PARALLEL_CORE_H__

#include <vector>
#include "mpi.h"

namespace apl
{

    class parallel_engine
    {
    private:

        static double start_time;
        static std::vector<MPI_Datatype> created_datatypes;

    public:

        parallel_engine(int* argc = nullptr, char*** argv = nullptr);
        virtual ~parallel_engine();

        void init_library(int* argc, char*** argv);
        void finalize_library();

        static void add_datatype(MPI_Datatype dt);

        static double get_start_time();
    };

}

#endif // __PARALLEL_CORE_H__
