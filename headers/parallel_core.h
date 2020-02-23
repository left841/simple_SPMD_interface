#ifndef __PARALLEL_CORE_H__
#define __PARALLEL_CORE_H__

#include "mpi.h"

namespace auto_parallel
{

    class parallel_engine
    {
    private:

        static double start_time;
        static size_t object_count;

    public:

        parallel_engine(int* argc = nullptr, char*** argv = nullptr);
        virtual ~parallel_engine();

        double get_start_time();

        virtual void execution() = 0;
        
    };

}

#endif // __PARALLEL_CORE_H__
