#ifndef __PARALLEL_CORE_H__
#define __PARALLEL_CORE_H__

#include "mpi.h"

namespace auto_parallel
{

    class parallel_engine
    {
    private:

        static double start_time;

    public:

        parallel_engine(int* argc = nullptr, char*** argv = nullptr);
        virtual ~parallel_engine();

        void init_library(int* argc, char*** argv);
        void finalize_library();

        static double get_start_time();
    };

}

#endif // __PARALLEL_CORE_H__
